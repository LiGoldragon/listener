/* Pulse owns microphone events. This worker owns their queue and one Ogg Opus file. */
#define _POSIX_C_SOURCE 200809L
#include "recording.h"
#include <errno.h>
#include <limits.h>
#include <ogg/ogg.h>
#include <opus.h>
#include <pulse/pulseaudio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    const listener_capture *setting;
    int control;
    int audio;
    pa_mainloop *loop;
    pa_context *context;
    pa_stream *stream;
    pa_io_event *stop_event;
    OpusEncoder *encoder;
    ogg_stream_state ogg;
    bool ogg_initialized;
    bool stopped;
    bool ready;
    int failure;
    int lookahead;
    float *queue;
    size_t samples;
    size_t capacity;
    int64_t granule;
    int64_t source_samples;
    int64_t packet_number;
    uint32_t page_packets;
    uint8_t pending_packet[1275 * 3 + 7];
    long pending_bytes;
    int64_t pending_granule;
} capture;

static bool write_all(int fd, const unsigned char *buffer, size_t bytes) {
    while (bytes != 0) {
        ssize_t count = write(fd, buffer, bytes);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        buffer += count;
        bytes -= (size_t)count;
    }
    return true;
}

static void little(unsigned char *target, uint32_t value, unsigned bytes) {
    for (unsigned index = 0; index < bytes; ++index) {
        target[index] = (uint8_t)(value >> (index * 8));
    }
}

static bool pages(capture *recording, bool flush) {
    ogg_page page;
    int result;
    while ((result = flush ? ogg_stream_flush(&recording->ogg, &page)
                          : ogg_stream_pageout(&recording->ogg, &page)) > 0) {
        if (!write_all(recording->audio, page.header, (size_t)page.header_len)
                || !write_all(recording->audio, page.body, (size_t)page.body_len)
                || (recording->setting->sync_each_page && fdatasync(recording->audio) != 0)) {
            recording->failure = LISTENER_FILE;
            return false;
        }
    }
    return result == 0;
}

static bool packet(capture *recording, unsigned char *bytes, long length, int64_t granule,
                   bool first, bool last, bool page_break) {
    ogg_packet packet = {
        .packet = bytes, .bytes = length, .b_o_s = first, .e_o_s = last,
        .granulepos = granule, .packetno = recording->packet_number++,
    };
    if (ogg_stream_packetin(&recording->ogg, &packet) != 0) {
        recording->failure = LISTENER_ENCODER;
        return false;
    }
    return pages(recording, page_break);
}

static bool headers(capture *recording) {
    unsigned char head[19] = "OpusHead";
    unsigned char tags[16] = "OpusTags";
    head[8] = 1;
    head[9] = (uint8_t)recording->setting->channels;
    little(head + 10, (uint32_t)recording->lookahead * (48000 / recording->setting->sample_rate), 2);
    little(head + 12, recording->setting->sample_rate, 4);
    return packet(recording, head, sizeof(head), 0, true, false, true)
        && packet(recording, tags, sizeof(tags), 0, false, false, true);
}

static bool emit_pending(capture *recording, bool last, int64_t granule) {
    if (recording->pending_bytes == 0) return true;
    bool boundary = ++recording->page_packets >= recording->setting->page_packets;
    if (boundary) recording->page_packets = 0;
    return packet(recording, recording->pending_packet, recording->pending_bytes, granule,
                  false, last, boundary || last);
}

static bool encode(capture *recording, const float *samples) {
    if (!emit_pending(recording, false, recording->pending_granule)) return false;
    int encoded = opus_encode_float(recording->encoder, samples, recording->setting->frame_samples,
                                    recording->pending_packet, sizeof(recording->pending_packet));
    if (encoded <= 0) {
        recording->failure = LISTENER_ENCODER;
        return false;
    }
    recording->granule += (int64_t)recording->setting->frame_samples *
        (48000 / recording->setting->sample_rate);
    recording->pending_bytes = encoded;
    recording->pending_granule = recording->granule;
    return true;
}

static bool encode_ready(capture *recording) {
    size_t frame = (size_t)recording->setting->frame_samples * (size_t)recording->setting->channels;
    size_t encoded = 0;
    while (recording->samples - encoded >= frame) {
        if (!encode(recording, recording->queue + encoded)) return false;
        encoded += frame;
    }
    recording->samples -= encoded;
    memmove(recording->queue, recording->queue + encoded, recording->samples * sizeof(float));
    return true;
}

static bool finish(capture *recording) {
    if (!encode_ready(recording)) return false;
    size_t channels = (size_t)recording->setting->channels;
    size_t frame = (size_t)recording->setting->frame_samples * channels;
    memset(recording->queue + recording->samples, 0, (frame - recording->samples) * sizeof(float));
    int64_t granule = (recording->source_samples + recording->lookahead) *
        (48000 / recording->setting->sample_rate);
    if (recording->pending_bytes == 0 || recording->granule < granule) {
        if (!encode(recording, recording->queue)) return false;
        memset(recording->queue, 0, frame * sizeof(float));
        while (recording->granule < granule) {
            if (!encode(recording, recording->queue)) return false;
        }
    }
    if (!emit_pending(recording, true, granule)) return false;
    if (fdatasync(recording->audio) != 0) {
        recording->failure = LISTENER_FILE;
        return false;
    }
    return true;
}

static void append(capture *recording, const void *data, size_t count) {
    size_t channels = (size_t)recording->setting->channels;
    if (count % channels != 0 || (data == NULL && recording->setting->interrupt_on_gap)) {
        recording->failure = LISTENER_GAP;
        return;
    }
    /* The queue is bounded. An oversized Pulse fragment is drained by frames. */
    const unsigned char *bytes = data;
    while (count != 0) {
        if (recording->samples == recording->capacity && !encode_ready(recording)) return;
        size_t remaining = recording->capacity - recording->samples;
        size_t batch = count < remaining ? count : remaining;
        float *target = recording->queue + recording->samples;
        if (bytes != NULL) {
            memcpy(target, bytes, batch * sizeof(float));
            bytes += batch * sizeof(float);
        } else {
            memset(target, 0, batch * sizeof(float));
        }
        recording->samples += batch;
        recording->source_samples += (int64_t)(batch / channels);
        count -= batch;
    }
}

static void readable(pa_stream *stream, size_t bytes, void *data) {
    (void)bytes;
    capture *recording = data;
    if (!recording->ready || recording->failure != 0) return;
    for (;;) {
        const void *samples;
        size_t size;
        if (pa_stream_peek(stream, &samples, &size) != 0) {
            recording->failure = LISTENER_MICROPHONE;
            return;
        }
        if (size == 0) return;
        if (size % sizeof(float) == 0) append(recording, samples, size / sizeof(float));
        else recording->failure = LISTENER_GAP;
        if (pa_stream_drop(stream) != 0 && recording->failure == 0) {
            recording->failure = LISTENER_MICROPHONE;
        }
        if (recording->failure != 0) return;
    }
}

static void overflow(pa_stream *stream, void *data) {
    (void)stream;
    capture *recording = data;
    if (!recording->ready || recording->failure != 0) return;
    append(recording, NULL, recording->setting->fragment_bytes / sizeof(float));
}

static void stream_state(pa_stream *stream, void *data) {
    capture *recording = data;
    switch (pa_stream_get_state(stream)) {
        case PA_STREAM_FAILED:
        case PA_STREAM_TERMINATED: recording->failure = LISTENER_MICROPHONE; break;
        default: break;
    }
}

static void context_state(pa_context *context, void *data) {
    capture *recording = data;
    switch (pa_context_get_state(context)) {
        case PA_CONTEXT_READY: {
            const listener_capture *setting = recording->setting;
            pa_sample_spec spec = {
                .format = PA_SAMPLE_FLOAT32NE, .rate = setting->sample_rate,
                .channels = (uint8_t)setting->channels,
            };
            pa_buffer_attr buffering = {
                .maxlength = setting->buffer_bytes, .tlength = UINT32_MAX, .prebuf = UINT32_MAX,
                .minreq = UINT32_MAX, .fragsize = setting->fragment_bytes,
            };
            recording->stream = pa_stream_new(context, setting->application_name, &spec, NULL);
            if (!recording->stream) {
                recording->failure = LISTENER_MICROPHONE;
                return;
            }
            pa_stream_set_state_callback(recording->stream, stream_state, recording);
            pa_stream_set_read_callback(recording->stream, readable, recording);
            pa_stream_set_overflow_callback(recording->stream, overflow, recording);
            if (pa_stream_connect_record(recording->stream, setting->source, &buffering,
                                         PA_STREAM_ADJUST_LATENCY) < 0) {
                recording->failure = LISTENER_MICROPHONE;
            }
            break;
        }
        case PA_CONTEXT_FAILED:
        case PA_CONTEXT_TERMINATED: recording->failure = LISTENER_MICROPHONE; break;
        default: break;
    }
}

static void stopping(pa_mainloop_api *api, pa_io_event *event, int fd, pa_io_event_flags_t flags,
                     void *data) {
    (void)api;
    (void)event;
    (void)fd;
    (void)flags;
    ((capture *)data)->stopped = true;
}

static bool encoder(capture *recording) {
    static const int application[] = {OPUS_APPLICATION_VOIP, OPUS_APPLICATION_AUDIO, OPUS_APPLICATION_RESTRICTED_LOWDELAY};
    static const int signal[] = {OPUS_AUTO, OPUS_SIGNAL_VOICE, OPUS_SIGNAL_MUSIC};
    static const int bandwidth[] = {OPUS_AUTO, OPUS_BANDWIDTH_NARROWBAND, OPUS_BANDWIDTH_MEDIUMBAND,
                                   OPUS_BANDWIDTH_WIDEBAND, OPUS_BANDWIDTH_SUPERWIDEBAND, OPUS_BANDWIDTH_FULLBAND};
    const listener_capture *setting = recording->setting;
    int result;
    recording->encoder = opus_encoder_create((opus_int32)setting->sample_rate, setting->channels,
                                              application[setting->opus_application], &result);
    if (result != OPUS_OK || !recording->encoder) return false;
    return opus_encoder_ctl(recording->encoder, OPUS_SET_BITRATE(setting->bitrate)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_VBR(setting->bitrate_control != 0)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_VBR_CONSTRAINT(setting->bitrate_control == 2)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_COMPLEXITY(setting->complexity)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_SIGNAL(signal[setting->signal])) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_BANDWIDTH(bandwidth[setting->bandwidth])) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_DTX(setting->discontinuous_transmission)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_INBAND_FEC(setting->forward_error_correction)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_SET_PACKET_LOSS_PERC(setting->packet_loss)) == OPUS_OK
        && opus_encoder_ctl(recording->encoder, OPUS_GET_LOOKAHEAD(&recording->lookahead)) == OPUS_OK;
}

static int serve(capture *recording) {
    const listener_capture *setting = recording->setting;
    size_t frame = (size_t)setting->frame_samples * (size_t)setting->channels;
    if (frame == 0 || setting->pending_frames > SIZE_MAX / (frame * sizeof(float))) return LISTENER_ENCODER;
    recording->capacity = setting->pending_frames * frame;
    recording->queue = calloc(recording->capacity, sizeof(float));
    if (!recording->queue || !encoder(recording)) return LISTENER_ENCODER;
    if (ogg_stream_init(&recording->ogg, setting->serial) != 0) return LISTENER_ENCODER;
    recording->ogg_initialized = true;
    recording->loop = pa_mainloop_new();
    if (!recording->loop) return LISTENER_MICROPHONE;
    pa_mainloop_api *api = pa_mainloop_get_api(recording->loop);
    recording->stop_event = api->io_new(api, recording->control,
                                        PA_IO_EVENT_INPUT | PA_IO_EVENT_HANGUP | PA_IO_EVENT_ERROR,
                                        stopping, recording);
    recording->context = pa_context_new(api, setting->application_name);
    if (!recording->stop_event || !recording->context) return LISTENER_MICROPHONE;
    pa_context_set_state_callback(recording->context, context_state, recording);
    if (pa_context_connect(recording->context, setting->server, PA_CONTEXT_NOAUTOSPAWN, NULL) < 0)
        return LISTENER_MICROPHONE;
    while (!recording->stopped && recording->failure == 0) {
        if (pa_mainloop_iterate(recording->loop, 1, NULL) < 0) {
            recording->failure = LISTENER_MICROPHONE;
            break;
        }
        if (recording->failure != 0) break;
        if (!recording->ready && recording->stream
            && pa_stream_get_state(recording->stream) == PA_STREAM_READY) {
            if (!headers(recording)) break;
            if (fdatasync(recording->audio) != 0) {
                recording->failure = LISTENER_FILE;
                break;
            }
            unsigned char begun = LISTENER_STOPPED;
            if (send(recording->control, &begun, 1, MSG_NOSIGNAL) != 1) {
                recording->failure = LISTENER_MICROPHONE;
                break;
            }
            recording->ready = true;
            readable(recording->stream, 0, recording);
        }
        if (recording->ready && !encode_ready(recording)) break;
    }
    int interruption = recording->failure;
    if (recording->ready && interruption != LISTENER_FILE && interruption != LISTENER_ENCODER) {
        if (!finish(recording)) return recording->failure;
    }
    return recording->ready ? interruption : (interruption == 0 ? LISTENER_MICROPHONE : interruption);
}

int listener_record(const listener_capture *setting, int control, int audio) {
    capture recording = {.setting = setting, .control = control, .audio = audio};
    int result = serve(&recording);
    if (!recording.ready) {
        unsigned char refusal = (unsigned char)result;
        (void)send(control, &refusal, 1, MSG_NOSIGNAL);
    }
    if (recording.stop_event) pa_mainloop_get_api(recording.loop)->io_free(recording.stop_event);
    if (recording.stream) pa_stream_unref(recording.stream);
    if (recording.context) pa_context_unref(recording.context);
    if (recording.loop) pa_mainloop_free(recording.loop);
    if (recording.ogg_initialized) ogg_stream_clear(&recording.ogg);
    if (recording.encoder) opus_encoder_destroy(recording.encoder);
    free(recording.queue);
    return result;
}

/* Finish a partial capture at its last complete page. The packet boundary
 * is checked independently of the writer's shutdown path. */
int listener_recover_audio(int audio) {
    ogg_sync_state sync;
    if (ogg_sync_init(&sync) != 0) return LISTENER_ENCODER;
    ogg_page page;
    off_t offset = 0, header_offset = 0, end = 0;
    unsigned char header[282];
    unsigned char *body = NULL;
    size_t header_bytes = 0, body_bytes = 0;
    int result = LISTENER_FILE;
    for (;;) {
        long read_page = ogg_sync_pageseek(&sync, &page);
        if (read_page > 0) {
            /* A job has one contiguous logical stream, written by this recorder. */
            if (page.header_len < 27 || (size_t)page.header_len > sizeof(header)) goto done;
            if ((page.header[5] & 4) != 0) {
                result = LISTENER_STOPPED;
                goto done;
            }
            size_t segments = page.header[26];
            if (segments != 0 && page.header[26 + segments] < 255 && ogg_page_granulepos(&page) > 0) {
                unsigned char *replacement = realloc(body, (size_t)page.body_len);
                if (!replacement) goto done;
                body = replacement;
                memcpy(body, page.body, (size_t)page.body_len);
                memcpy(header, page.header, (size_t)page.header_len);
                header_bytes = (size_t)page.header_len;
                body_bytes = (size_t)page.body_len;
                header_offset = offset;
                end = offset + read_page;
            }
            offset += read_page;
            continue;
        }
        if (read_page < 0) break;
        char *buffer = ogg_sync_buffer(&sync, 4096);
        if (!buffer) goto done;
        ssize_t count = read(audio, buffer, 4096);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) goto done;
        if (count == 0) break;
        if (ogg_sync_wrote(&sync, count) != 0) goto done;
    }
    if (end == 0) goto done;
    header[5] |= 4;
    page = (ogg_page){.header = header, .header_len = (long)header_bytes,
                      .body = body, .body_len = (long)body_bytes};
    ogg_page_checksum_set(&page);
    if (lseek(audio, header_offset, SEEK_SET) < 0 || !write_all(audio, header, header_bytes)
        || ftruncate(audio, end) != 0 || fdatasync(audio) != 0) goto done;
    result = LISTENER_STOPPED;
done:
    free(body);
    ogg_sync_clear(&sync);
    return result;
}

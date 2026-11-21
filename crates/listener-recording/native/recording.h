#ifndef LISTENER_RECORDING_H
#define LISTENER_RECORDING_H

#include <stdint.h>

typedef struct {
    const char *server;
    const char *application_name;
    const char *source;
    uint32_t sample_rate;
    int32_t channels;
    uint32_t fragment_bytes;
    uint32_t buffer_bytes;
    uint32_t pending_frames;
    int32_t frame_samples;
    int32_t opus_application;
    int32_t bitrate;
    int32_t bitrate_control;
    int32_t complexity;
    int32_t signal;
    int32_t bandwidth;
    int32_t discontinuous_transmission;
    int32_t forward_error_correction;
    int32_t packet_loss;
    uint32_t page_packets;
    int32_t sync_each_page;
    int32_t interrupt_on_gap;
    int32_t serial;
} listener_capture;

/* Zero is a successful handshake or stop. Codes 1–4 name the edge that refused. */
enum listener_recording_result {
    LISTENER_STOPPED, LISTENER_MICROPHONE, LISTENER_FILE, LISTENER_ENCODER, LISTENER_GAP
};

int listener_record(const listener_capture *, int control, int audio);
int listener_recover_audio(int audio);

#endif

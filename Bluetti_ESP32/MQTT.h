#ifndef MQTT_H
#define MQTT_H
#include "Arduino.h"
#include "DeviceType.h"

extern void publishTopic(enum field_names field_name, String value);
// Queue a field/value from the BLE task; actually published later on the main loop.
extern void enqueuePublish(enum field_names field_name, String value);
// Drain a few queued publishes (call from the main loop only).
extern void handlePublishQueue();
extern void publishHAConfig();
extern void publishDeviceState();
extern void publishDeviceStateStatus();
extern void deviceServoPress(int degree);
extern void handleMQTT();
extern void initMQTT();
extern bool isMQTTconnected();
extern int getPublishErrorCount();
unsigned long getLastMQTTMessageTime();
unsigned long getLastMQTTDeviceStateMessageTime();
unsigned long getLastMQTTDeviceStateStatusMessageTime();

#endif

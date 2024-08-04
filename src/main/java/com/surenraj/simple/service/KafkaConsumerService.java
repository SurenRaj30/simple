package com.surenraj.simple.service;

import com.surenraj.simple.pojo.ResponseObj;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaConsumerService {
    private final List<ResponseObj> messages = new ArrayList<>();

    @KafkaListener(topics = "first_topic", groupId = "group_id")
    public void consume(ResponseObj message) {
        synchronized (messages) {
            messages.add(message);
        }
    }
    public List<ResponseObj> getMessages() {
        synchronized (messages) {
            return new ArrayList<>(messages);
        }
    }
}

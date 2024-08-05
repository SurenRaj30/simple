package com.surenraj.simple.service;

import com.surenraj.simple.pojo.ResponseObj;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class KafkaConsumerService {
    //to store sent messages
    private final List<ResponseObj> messages = new ArrayList<>();

    // specify which topic and group kafka should listen to
    // will be auto invoked when a message is sent to this topic
    @KafkaListener(topics = "first_topic", groupId = "group_id")
    public void consume(ResponseObj message) {
        // synchronized keyword: ensures only one thread can access a block of code or a method at a time
        // in this case it controls access to messages list
        // to avoid multiple thread making changes to messages list
        synchronized (messages) {
            messages.add(message);
        }
    }

    // get all the message sent to a topic
    public List<ResponseObj> getMessages() {
        synchronized (messages) {
            return new ArrayList<>(messages);
        }
    }
}

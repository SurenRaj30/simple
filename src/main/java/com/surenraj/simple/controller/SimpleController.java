package com.surenraj.simple.controller;

import com.surenraj.simple.pojo.ResponseObj;
import com.surenraj.simple.service.KafkaConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@Validated
@RequestMapping("/api/v1")
public class SimpleController {
    //logger
    private static final Logger logger = LoggerFactory.getLogger(SimpleController.class);
    private static final String TOPIC = "first_topic";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    private KafkaConsumerService kafkaConsumerService;
    @PostMapping("/produce")
    public ResponseEntity<String> produceRequest(@RequestBody ResponseObj req) {
       //debugging
        logger.info("Received request with name: {} and age: {}", req.getName(), req.getAge());
        kafkaTemplate.send(TOPIC, req);
       //return success message
        return new ResponseEntity<>("Message sent to Kafka topic successfully", HttpStatus.OK);
    }
    @GetMapping("/consume")
    public List<ResponseObj> consumeMessages() {
        return kafkaConsumerService.getMessages();
    }
    //method to handle exception
    @ExceptionHandler(Exception.class)
    public ResponseEntity<String> handleException(Exception e) {
        logger.error("An error occurred: ", e);
        return new ResponseEntity<>("An error occurred while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);

    }
}

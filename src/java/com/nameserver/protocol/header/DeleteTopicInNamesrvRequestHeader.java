package com.nameserver.protocol.header;


import com.nameserver.remoting.CommandCustomHeader;
import com.nameserver.remoting.annotaion.CFNotNull;
import com.nameserver.remoting.excpetion.RemotingCommandException;

public class DeleteTopicInNamesrvRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}

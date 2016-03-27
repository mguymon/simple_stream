package com.tobedevoured.json;

import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonStreamHandler implements ContentHandler {

    private static final Logger logger = LoggerFactory.getLogger(JsonStreamHandler.class);

    int openEntity = 0;

    enum STATE { START_JSON, END_JSON, START_OBJECT, END_OBJECT, START_ARRAY, END_ARRAY; }

    private STATE status;

    @Override
    public void startJSON() throws ParseException, IOException {
        setStatus(STATE.START_JSON);
        logger.debug("startJson");
    }

    @Override
    public void endJSON() throws ParseException, IOException {
        setStatus(STATE.END_JSON);
        logger.debug("endJson");
    }

    @Override
    public boolean startObject() throws ParseException, IOException {
        setStatus(STATE.START_OBJECT);

        openEntity++;
        logger.debug("startObject of {}", openEntity);
        return true;
    }

    @Override
    public boolean endObject() throws ParseException, IOException {
        setStatus(STATE.END_OBJECT);

        logger.debug("endObject of {}", openEntity);
        openEntity--;
        return openEntity > 0;
    }

    @Override
    public boolean startObjectEntry(String key) throws ParseException, IOException {
        return true;
    }

    @Override
    public boolean endObjectEntry() throws ParseException, IOException {
        return true;
    }

    @Override
    public boolean startArray() throws ParseException, IOException {
        setStatus(STATE.START_ARRAY);

        openEntity++;
        logger.debug("startArray of {}", openEntity);
        return true;
    }

    @Override
    public boolean endArray() throws ParseException, IOException {
        setStatus(STATE.END_ARRAY);

        logger.debug("endArray of {}", openEntity);
        openEntity--;
        return openEntity > 0;
    }

    @Override
    public boolean primitive(Object value) throws ParseException, IOException {
        return true;
    }

    public STATE getStatus() {
        return status;
    }

    public void setStatus(STATE status) {
        this.status = status;
    }
}

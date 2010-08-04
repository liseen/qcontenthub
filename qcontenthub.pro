TEMPLATE = app
TARGET=qcontenthubd

SOURCES += qcontenthub_rpc.cpp
SOURCES += qurlqueue_rpc.cpp main.cpp
HEADERS += qcontenthub_rpc.h qurlqueue_rpc.h qcontenthub.h

LIBS=-lmsgpack-rpc

CONFIG += debug

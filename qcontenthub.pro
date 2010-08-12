TEMPLATE = app

TARGET=qcontenthubd

SOURCES += qcontenthub_rpc.cpp
SOURCES += qurlqueue_rpc.cpp main.cpp
HEADERS += qcontenthub_rpc.h qurlqueue_rpc.h qcontenthub.h

CONFIG += release
QT -= gui core

LIBS = -lmsgpack-rpc -lrt

INSTALLDIR=/opt/qcontent/3rdparty/

target.path  = $$INSTALLDIR/bin

INSTALLS += target



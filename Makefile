JAVAC = javac -Xlint:unchecked
sources = $(shell find . -type f -name '*.java')
classes = $(sources:.java=.class)

all : $(classes)

clean :
	rm -f $(classes)

%.class : %.java
	$(JAVAC) $<

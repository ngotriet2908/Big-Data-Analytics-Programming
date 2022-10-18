BUILDDIR=build

JC=javac -d $(BUILDDIR)
JV=java -ea -cp $(BUILDDIR)

default: ReservoirSampling

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/%.class: %.java $(BUILDDIR)
	$(JC) $<

.PHONY: ReservoirSampling HyperLogLog BloomFilter
ReservoirSampling: $(BUILDDIR)/GZIPReader.class $(BUILDDIR)/ReservoirSampling.class
	$(JV) $@
HyperLogLog: $(BUILDDIR)/GZIPReader.class $(BUILDDIR)/HyperLogLog.class
	$(JV) $@
BloomFilter: $(BUILDDIR)/GZIPReader.class $(BUILDDIR)/BloomFilter.class
	$(JV) $@

.PHONY: clean
clean:
	rm -r $(BUILDDIR)



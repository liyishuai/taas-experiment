#gray.pdf: result.pdf
#	-unalias gs
#	gs -sDEVICE=pdfwrite -sProcessColorModel=DeviceGray -sColorConversionStrategy=Gray -dOverrideICC -o $@ -f $^

result.pdf: ribbon.py tso.csv taas.csv
	python3 $<

throughput-line-3az.pdf: throughput-line-3az.py
	python3 $<

latency-bar-3az.pdf: latency-bar-3az.py
	python3 $<

.PHONY: throughput-line-3az.pdf latency-bar-3az.pdf

3az: throughput-line-3az.pdf latency-bar-3az.pdf 

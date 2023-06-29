#gray.pdf: result.pdf
#	-unalias gs
#	gs -sDEVICE=pdfwrite -sProcessColorModel=DeviceGray -sColorConversionStrategy=Gray -dOverrideICC -o $@ -f $^

result.pdf: ribbon.py tso.csv taas.csv
	python3 $<

throuput-line-3az.pdf: throuput-line-3az.py
	python3 $<

latency-bar-3az.pdf: latency-bar-3az.py
	python3 $<

.PHONY: throuput-line-3az.pdf latency-bar-3az.pdf

3az: throuput-line-3az.pdf latency-bar-3az.pdf 

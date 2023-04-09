gray.pdf: result.pdf
	gs -sDEVICE=pdfwrite -sProcessColorModel=DeviceGray -sColorConversionStrategy=Gray -dOverrideICC -o $@ -f $^

result.pdf: ribbon.py taas.csv tso.csv
	python3 $<

clean:
	$(RM) gray.pdf result.pdf

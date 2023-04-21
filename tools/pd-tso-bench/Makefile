gray.pdf: result.pdf
	-unalias gs
	gs -sDEVICE=pdfwrite -sProcessColorModel=DeviceGray -sColorConversionStrategy=Gray -dOverrideICC -o $@ -f $^

result.pdf: ribbon.py tso.csv taas.csv
	python3 $<
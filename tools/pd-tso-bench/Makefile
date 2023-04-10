gray.pdf: result.pdf
	gs -sDEVICE=pdfwrite -sProcessColorModel=DeviceGray -sColorConversionStrategy=Gray -dOverrideICC -o $@ -f $^

	python3 $<


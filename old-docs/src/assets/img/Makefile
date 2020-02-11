rasterize:
	$(foreach h,36 48 64 128,$(foreach svg,$(wildcard ./*.svg),$(shell inkscape $(svg) --without-gui --export-png="$(basename $(svg))-$(h).png" --export-height=$(h))))

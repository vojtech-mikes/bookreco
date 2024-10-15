.PHONY: req

req:
	conda env export > environment.yml

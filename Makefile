.PHONY: req dev

req:
	conda env export > environment.yml

dev:
	flask --app server --debug run

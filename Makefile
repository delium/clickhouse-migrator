setup-dev:
	pyenv local 3.6.15 3.7.12 3.8.6 3.9.9 3.10.0 || true
	pip install tox-pyenv

test:
	tox

fix:
	tox -e isort
	tox -e black

build:
	tox -e build_wheel

publish:
	tox -e upload

open-coverage:
	xdg-open htmlcov/index.html || open htmlcov/index.html

docker-compose-up:
	cd dev && make configure up

docker-compose-down:
	cd dev && make down clean


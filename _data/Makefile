all: clean setup build_frontend build

prod: all

build: build_frontend
clean:
	rm -rf app/node_modules
	rm -rf app/dist
	rm -rf app/build
	

setup:
	mkdir -p app/build

build_frontend:
	cd app && . "$$HOME/.nvm/nvm.sh" && nvm use 20.19.2 && npm install && npm run build
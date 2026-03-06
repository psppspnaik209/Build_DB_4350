APP := kvstore.exe
GRADEBOT := .\gradebot.exe
PROJECT := project-1
RUN_PATH := $(CURDIR)\$(APP)

.PHONY: all build clean-db grade run

all: build clean-db grade

build:
	go build -o $(APP) .

clean-db:
	powershell -Command "if (Test-Path 'data.db') { Remove-Item 'data.db' }"

grade: build clean-db
	$(GRADEBOT) $(PROJECT) --dir "$(CURDIR)" --run "$(RUN_PATH)"

run: build
	.\$(APP)

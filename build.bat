@echo off
echo Starting compilation...
pyinstaller --noconfirm --onefile --windowed ^
    --name "1024ImageCrawler" ^
    --icon "logo.ico" ^
    --add-data "logo.png;." ^
    --add-data "logo.ico;." ^
    --hidden-import "PyQt6" ^
    --hidden-import "PyQt6.QtWebEngineWidgets" ^
    --hidden-import "PyQt6.QtWebEngineCore" ^
    --hidden-import "PyQt6.QtWebChannel" ^
    --hidden-import "PyQt6.QtNetwork" ^
    --hidden-import "PyQt6.QtPrintSupport" ^
    --hidden-import "requests" ^
    --hidden-import "bs4" ^
    main.py
echo Compilation finished.
exit

from PySide6.QtGui import QAction,QIcon
from PySide6.QtWidgets import (QWidget,QApplication,QMainWindow,QVBoxLayout,QHBoxLayout,QSpinBox,
                               QPushButton,QToolButton,QLabel,QScrollArea,
                               QLineEdit,QFormLayout,QMessageBox,QFileDialog)
from PySide6.QtCore import QSize,Qt
import sys, subprocess, json
from seek_crawler import Path, BASE_DIR, main

class MyMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # meta set up
        self.setWindowTitle('Seeker v1.0')

        # set window icon
        icon_path=BASE_DIR/"data/icon.png"
        self.setWindowIcon(QIcon(str(icon_path)))
        self.setMinimumSize(QSize(800,600))

        # define layout
        # init layout
        main_layout=QVBoxLayout()
        body_layout=QFormLayout()
        page_layout=QHBoxLayout()
        page_widget=QWidget()
        expiry_layout=QHBoxLayout()
        expiry_widget=QWidget()
        save_path_layout=QHBoxLayout()
        btn_layout=QHBoxLayout()
        display_layout=QVBoxLayout()

        # define kw
        kw_label=QLabel('Keyword: ')
        self.kw=QLineEdit()
        self.kw.setPlaceholderText('Enter job keyword or leave blank for all (case insensitive)')

        classification_label=QLabel('Subcategory: ')
        self.classification=QLineEdit()
        self.classification.setPlaceholderText('Enter subcategory or leave blank for all (case insensitive)')

        location_label=QLabel('Location: ')
        self.location=QLineEdit()
        self.location.setPlaceholderText('Enter location or leave blank for all (case insensitive)')

        body_layout.addRow(kw_label,self.kw)
        body_layout.addRow(classification_label,self.classification)
        body_layout.addRow(location_label,self.location)

        # define numeric args
        page_label=QLabel('Page(s) to parse: ')
        self.pageNum=QSpinBox()
        self.pageNum.setRange(1,20)
        self.pageNum.setValue(3)

        expiry_label=QLabel('Expiry days: ')
        self.expiry=QSpinBox()
        self.expiry.setRange(2,31)
        self.expiry.setValue(14)

        page_layout.addWidget(page_label)
        page_layout.addWidget(self.pageNum)
        page_widget.setLayout(page_layout
                              )
        expiry_layout.addWidget(expiry_label)
        expiry_layout.addWidget(self.expiry)
        expiry_widget.setLayout(expiry_layout)

        body_layout.addRow(page_widget,expiry_widget)

        # define save path
        with open(BASE_DIR / "data/args.json",'r') as rf:
            self.SAVE_DIR=json.load(rf)['save_path'] or BASE_DIR

        path_label=QLabel('Excel saved at: ')
        self.save_path=QLineEdit()
        self.save_path.setText(str(self.SAVE_DIR))
        self.save_path.setEnabled(False)

        select_folder_btn=QPushButton('...')
        select_folder_btn.setMaximumSize(25,25)
        select_folder_btn.setToolTip('Select folder for excel')
        select_folder_btn.clicked.connect(self.open_folder_selector)

        open_folder_btn=QToolButton()
        open_folder_btn.setIcon(QIcon(str(BASE_DIR / 'data/folder.png')))
        open_folder_btn.setMaximumSize(25,25)
        open_folder_btn.setToolTip('Open folder')
        open_folder_btn.clicked.connect(self.open_folder)

        save_path_layout.addWidget(self.save_path)
        save_path_layout.addWidget(select_folder_btn)
        save_path_layout.addWidget(open_folder_btn)
        body_layout.addRow(path_label,save_path_layout)

        # function buttons
        self.execute_btn=QPushButton('Go seeking!')
        self.execute_btn.clicked.connect(self.execute_seeker)

        btn_layout.addWidget(self.execute_btn)

        # display section
        self.scroll=QScrollArea()
        self.display=QLabel('Waiting...')
        self.display.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.display.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.scroll.setWidget(self.display)
        self.scroll.setWidgetResizable(True)
        display_layout.addWidget(self.scroll)

        # define widgets
        # menu bar
        main_menu=self.menuBar()
        about_menu=main_menu.addMenu('File')
        about_action=QAction('About',self)
        about_action.triggered.connect(self.show_about_info)
        about_menu.addAction(about_action)

        # statusBar
        self.statusbar=self.statusBar()
        self.update_status_bar()

        # define container
        container=QWidget()

        # nest layouts
        main_layout.addLayout(body_layout)
        main_layout.addLayout(btn_layout)
        main_layout.addLayout(display_layout)

        container.setLayout(main_layout)
        self.setCentralWidget(container)

        # auto fill last time args
        self.auto_fill()

    def show_about_info(self):
        self.display.clear()
        about_info = '''
            =========================================== version 1.0 ==========================================
            This is a software to scrape listed jobs on seek.com.au by keywords in job type, subcategory and location.

            It parses all jobs and ranked them by posted date, split by contract types.

            It then reads from a local excel file and combines records.
            
            The combination is done by removing duplicates and expired jobs according to user defined expiry days.

            Seeker also utilises Python multi-threading to speed up operations.

            @ 2024-02-19 Developed and distributed by Ranco Xu (https://github.com/RancoX)
        '''
        self.update_display_text(about_info)

    def get_current_path(self):
        return BASE_DIR
    
    def open_folder_selector(self,s):
        self.save_path.setEnabled(True)
        dialog = QFileDialog(self)
        dialog.setDirectory(self.save_path.text())
        dialog.setFileMode(QFileDialog.FileMode.Directory)
        dialog.setViewMode(QFileDialog.ViewMode.List)
        if dialog.exec():
            foldernames = dialog.selectedFiles()
            if foldernames:
                self.SAVE_DIR = Path(foldernames[0])
                self.save_path.setText(str(self.SAVE_DIR))
        self.save_path.setEnabled(False)

    def open_folder(self,s):
        folder_path = Path(self.SAVE_DIR)
        subprocess.Popen(f'explorer {folder_path}', shell=True)

    def update_display_text(self,new_text:str,clean=False):
        if clean:
            self.display.setText(new_text)
        else:
            self.display.setText(self.display.text() + '\n' + new_text)
            self.scroll.verticalScrollBar().setValue(self.scroll.verticalScrollBar().maximum())
        self.display.repaint()

    def update_status_bar(self,msg='Ready'):
        self.statusbar.showMessage(msg)
        self.statusbar.repaint()

    def execute_seeker(self,s):
        # get parameters
        kwargs={
        'BASE_URL':r'https://www.seek.com.au',
        'headers':{'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'accept':'text/html; charset=utf-8'},
        'search_pattern':'article[data-card-type="JobCard"] > div > div > div[class="y735df0 _1akoxc50 _1akoxc56"]',
        
        # note keyword doesn't always go along with subclassification
        'keyword':self.kw.text(),
        'subclassification':self.classification.text(),
        'location':self.location.text(),
        'pages_to_parse':self.pageNum.value(),
        'expiry':self.expiry.value(),
        'SAVE_DIR':self.SAVE_DIR,
    }
        # execute seeker
        return_str=main(**kwargs)

        # open destination
        self.show_yes_no_dialog(return_str)

        # save kwargs used this time
        self.save_args()

    def show_yes_no_dialog(self,text):
        reply = QMessageBox.question(self, "Done scraping", "Do you want to open file saved location?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

        if reply == QMessageBox.StandardButton.Yes:
            self.open_folder(None)
        
        self.update_display_text(text,True)
        self.update_status_bar('Finished. Ready for next seeking...')

    def auto_fill(self):
        with open(BASE_DIR / "data/args.json","r") as rf:
            self.kwargs=json.load(rf)
        
        # auto fill elements in UI
        for k,v in self.kwargs.items():
            if v:
                target=getattr(self,k)
                try:
                    target.setText(v)
                except:
                    target.setValue(v)
    
    def save_args(self):
        changed_flag=False
        if self.kwargs['kw']!=self.kw.text():
            self.kwargs['kw']=self.kw.text()
            changed_flag=True
        
        if self.kwargs['classification']!=self.classification.text():
            self.kwargs['classification']=self.classification.text()
            changed_flag=True
        
        if self.kwargs['location']!=self.location.text():
            self.kwargs['location']=self.location.text()
            changed_flag=True

        if self.kwargs['pageNum']!=self.pageNum.value():
            self.kwargs['pageNum']=self.pageNum.value()
            changed_flag=True

        if self.kwargs['expiry']!=self.expiry.value():
            self.kwargs['expiry']=self.expiry.value()
            changed_flag=True

        if self.kwargs['save_path']!=self.save_path.text():
            self.kwargs['save_path']=str(Path(self.save_path.text()))
            changed_flag=True

        if changed_flag:
            with open(BASE_DIR / "data/args.json",'w') as wf:
                json.dump(self.kwargs,wf,indent=4)

            show_msg=f"New query parameters: {self.kwargs['kw']}, subcategory: {self.kwargs['classification']}, loaction: {self.kwargs['location']}, page(s) to parse: {self.kwargs['pageNum']}, expiry days: {self.kwargs['expiry']} have been saved."
            self.update_display_text(show_msg)
    
if __name__ == '__main__':
    app=QApplication(sys.argv)

    window=MyMainWindow()
    window.show()

    app.exec()

# pyinstaller --noconsole --icon="C:\Users\RancoXu\OneDrive - Argyle Capital Partners Pty Ltd\Desktop\Ranco\Python\Seeker\data\icon.png" --noconfirm "C:\Users\RancoXu\OneDrive - Argyle Capital Partners Pty Ltd\Desktop\Ranco\Python\Seeker\seeker.py" --paths "C:\Users\RancoXu\OneDrive - Argyle Capital Partners Pty Ltd\Desktop\Ranco\Python\Seeker\venv_seeker\Lib\site-packages"  --add-data 'seek_crawler.py;.' --add-data 'data\folder.png;.\data' --add-data 'data\args.json;.\data'
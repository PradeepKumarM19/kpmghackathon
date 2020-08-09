import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from ttkthemes import ThemedTk

import argparse
from interface import Interface

class Root(ThemedTk):
    def __init__(self, args):
        super(Root, self).__init__(theme="arc")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.config_file = args.config_file
        self.title("SCD files import")
        self.minsize(300, 150)

        self.labelFrame = ttk.LabelFrame(self, text = "Import files")
        self.labelFrame.grid(column = 4, row = 4, padx=20, pady = 20,sticky=tk.N+tk.S+tk.E+tk.W)

        self.labelFrame_files = ttk.LabelFrame(self, text = "Selected files")
        self.labelFrame_files.grid(column = 5, row = 4, padx=100, pady = 20, sticky=tk.N+tk.S+tk.E+tk.W)

        self.buttons()
 
    def buttons(self):
        self.button = ttk.Button(self.labelFrame, text = "Select file/s", command = self.fileDialog)
        self.button.grid(column = 2, row = 1,padx=10, pady=20, sticky=tk.N+tk.S+tk.E+tk.W)
        self.button = ttk.Button(self.labelFrame, text = "Import", command = self.run_scd)
        self.button.grid(column = 1, row = 3,padx=10, pady=20, sticky=tk.N+tk.S+tk.E+tk.W)
        self.button = ttk.Button(self.labelFrame, text = "Cancel", command = self.quit)
        self.button.grid(column = 3, row = 3,padx=10, pady=20, sticky=tk.N+tk.S+tk.E+tk.W)

    def fileDialog(self):
 
        self.filename = filedialog.askopenfilenames(
            initialdir =  "/",
            title = "Select file(s)",
            filetype = (
                ("csv files","*.csv"),
                ("tsv files","*.tsv"),
                ("dat files","*.dat"),
                ("all files","*.*")
            )
        )

        for i in range(len(self.filename)):

            self.label = ttk.Label(self.labelFrame_files, text = "Hello")
            self.label.grid(column = 2, row = i+1, sticky=tk.N+tk.S+tk.E+tk.W)
            self.label.configure(text = self.filename[i])

    def run_scd(self):
        self.destroy()
        Interface.execute_config(self.filename, self.config_file)


def parse_args():
    parser = argparse.ArgumentParser(description='Process arguments')
    parser.add_argument("--config_file", help='Should provide config file path/location')
    return parser.parse_args()

if __name__ == "__main__":
    root = Root(parse_args())
    root.mainloop()


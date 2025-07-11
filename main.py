import tkinter as tk
from tkinter import filedialog
from functools import partial
import pandas, duckdb, numpy, math

class gun_broker_inventory:

    def __init__(self, bound_book_csv: pandas.DataFrame, items_ended_csv: pandas.DataFrame, selling_items_csv: pandas.DataFrame):
        # no filters
        self.bb: pandas.DataFrame = bound_book_csv

        # only filter for last 60 days and not sold
        self.ie: pandas.DataFrame = items_ended_csv
        
        # no filters                                                                      
        self.si: pandas.DataFrame = selling_items_csv
        

    def write_query(self, query: str) -> pandas.DataFrame:
        bb = self.bb
        si = self.si
        ie = self.ie

        return duckdb.sql(query).to_df()                  
    
    def get_selling_duplicates(self) -> pandas.DataFrame:
        df = self.si[self.si.duplicated(subset=['SerialNumber'], keep=False)].sort_values(by='SerialNumber').dropna(subset=['SerialNumber'])
        df = df[df["SerialNumber"] != "MSNUS"]
        df = df[df["SerialNumber"] != "MSNNS"]
        return df

    def get_selling_already_disposed(self) -> pandas.DataFrame:
        new = self.bb[self.bb["DispositionDate"].isna()]
        si = self.si
        si = si.dropna(subset=['SerialNumber'])

        df = duckdb.sql(
            """
            SELECT A.Title, A.SerialNumber
            FROM si AS A
            LEFT OUTER JOIN new AS B
            ON A.SerialNumber = B.SerialNumber
            WHERE B.SerialNumber IS NULL;
            """
        ).to_df()
        df = df[df["SerialNumber"] != "MSNUS"]
        df = df[df["SerialNumber"] != "MSNNS"]
        return df

    def descriptions_to_relist(self):
        
        a = self.write_query(
            """
            SELECT A.Title, A.SerialNumber
            FROM si A
            WHERE A.SerialNumber != 'None';
            """
        ).drop_duplicates()
    
        b = self.write_query(
            """
            SELECT A.Title, A.SerialNumber
            FROM ie A
            WHERE A.SerialNumber != 'None';
            """
        ).drop_duplicates()

        c = pandas.concat([pandas.concat([a, b]).drop_duplicates(), a]).drop_duplicates(keep=False)

        bb = self.bb

        d = duckdb.sql(
            """
            SELECT A.Title, A.SerialNumber
            FROM c AS A
            RIGHT JOIN (SELECT SerialNumber, DispositionDate FROM bb WHERE DispositionDate IS NULL) AS B
            ON A.SerialNumber = B.SerialNumber
            WHERE Title IS NOT NULL;
            """
        ).to_df()

        for i in a["SerialNumber"]:
            d = d[d["SerialNumber"] != i]

        d.drop_duplicates(subset='SerialNumber', keep='first', inplace=True) 
        
        return d
    

class idk:
    def __init__(self):
            self.df_bb = pandas.DataFrame()
            self.df_selling = pandas.DataFrame()
            self.df_ended = pandas.DataFrame()

            # Create the main Tkinter window
            self.root = tk.Tk()
            self.root.title("Gunbroker Checker")

            self.text_display = tk.Text(self.root, height=40, width=100)
            self.text_display.pack()

            # Create the "Upload CSV" button
            self.upload_button_bb = tk.Button(self.root, text="Bound Book CSV", command=partial(self.upload_csv, "bb"))
            self.upload_button_bb.pack(side=tk.TOP, padx=10, pady=10)

            # Create the "Upload CSV" button
            self.upload_button_selling = tk.Button(self.root, text="Selling CSV", command=partial(self.upload_csv, "s"))
            self.upload_button_selling.pack(side=tk.TOP, padx=10, pady=10)

            # Create the "Upload CSV" button
            self.upload_button_ended = tk.Button(self.root, text="Ended CSV", command=partial(self.upload_csv, "e"))
            self.upload_button_ended.pack(side=tk.TOP, padx=10, pady=10)

            # Create the "Print Columns" button
            self.print_button = tk.Button(self.root, text="Print Columns", command=self.submit)
            self.print_button.pack(side=tk.BOTTOM, padx=10, pady=10)

            # Create a status label
            self.status_label = tk.Label(self.root, text="")
            self.status_label.pack(pady=5)

    def upload_csv(self, btn_name):
        filepath = filedialog.askopenfilename(defaultextension=".csv", filetypes=[("CSV Files", "*.csv"), ("CSV Files Uppercase", "*.CSV")])

        if filepath:
            try:
                if (btn_name == "bb"):
                    self.df_bb = pandas.read_csv(filepath, encoding='utf-8', low_memory=False, on_bad_lines="skip")
                    self.df_bb = self.df_bb[["Serial Number", "Disposition Date", "Manufacturer/PMF"]]
                    self.df_bb = self.df_bb.rename(columns={'Serial Number': 'SerialNumber', 'Disposition Date':'DispositionDate'})
                    self.df_bb.loc[:, 'SerialNumber'] = self.df_bb["SerialNumber"].str.upper()
                    self.status_label.config(text="Bound Book CSV loaded successfully!")

                elif (btn_name == "s"):
                    self.df_selling = pandas.read_csv(filepath, encoding='utf-8')
                    self.df_selling = self.df_selling[["Title", "SerialNumber"]]
                    self.df_selling.loc[:, 'SerialNumber'] = self.df_selling["SerialNumber"].str.upper()
                    self.status_label.config(text="Selling CSV loaded successfully!")

                elif (btn_name == "e"):
                    self.df_ended = pandas.read_csv(filepath, encoding='utf-8')
                    self.df_ended = self.df_ended[["Title", "SerialNumber"]]
                    self.df_ended.loc[:, 'SerialNumber'] = self.df_ended["SerialNumber"].str.upper()
                    self.status_label.config(text="Ended CSV loaded successfully!")

            except Exception as e:
                self.status_label.config(text=f"Error loading CSV: {e}")

    def submit(self):
        gbi = gun_broker_inventory(self.df_bb, self.df_ended, self.df_selling)
    
        text = []

        df = gbi.get_selling_duplicates()
        if not df.empty:
            text.append("These might be duplicate firearms:\n")
            text.append(df.to_string(index=False, header=False))
        else:
            text.append("There are no duplicate firearms!\n")

        df = gbi.get_selling_already_disposed()
        if not df.empty:
            text.append("\n\nThese might be disposed firearms:\n")
            text.append(df.to_string(index=False, header=False))
        else:
            text.append("\n\nThere are no disposed firearms!\n")

        df = gbi.descriptions_to_relist()
        if not df.empty:
            text.append("\n\nThese firearms need to be relisted:\n")
            text.append(df.to_string(index=False, header=False))
        else:
            text.append("\n\nThere are no firearms that need to be relisted!\n")

        self.text_display.delete("1.0", tk.END)  # Clear existing text
        self.text_display.insert(tk.END, "\n".join(text)) # Insert new text
    def run(self):
        
        # Start the Tkinter event loop
        self.root.mainloop()

if __name__ == "__main__":
    idk().run()

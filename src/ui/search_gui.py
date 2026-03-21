import tkinter as tk
from tkinter import messagebox
from src.search.searcher import Searcher
import webbrowser
import time

class SearchApp:
    """
    Graphical user interface for the ZotSearch search engine.

    This application provides a simple desktop front end for interactive
    querying. It loads the search index once at startup, accepts user
    queries through a text entry field, displays ranked URLs, and allows
    users to open results directly in a web browser.
    """

    def __init__(self, root):
        """
        Initialize the search GUI and load the retrieval engine.

        The UI is built immediately, and the Searcher is created at startup
        so the index is ready before the user submits a query.

        :param root: Root Tkinter window.
        :type root: tk.Tk
        :return: None
        """

        self.root = root
        self.root.title("UCI ZotSearch Engine")
        self.root.geometry("800x600")

        # Searcher instance is created once and reused for all queries.
        self.searcher = None

        # Ensure the postings file handle is closed properly when the window exits.
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Display a temporary loading message while the index structures
        # are being loaded into memory.
        self.status_loading = tk.Label(
            root,
            text="Loading index, please wait...",
            fg="blue",
            font=("Arial", 11)
        )
        self.status_loading.pack(pady=10)
        self.root.update()

        # Build the search engine backend. If loading fails, the UI exits
        # cleanly after showing an error dialog.
        try:
            self.searcher = Searcher()
            self.status_loading.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"Could not load index:\n{e}")
            self.root.destroy()
            return

        # Main title/header shown at the top of the window.
        header = tk.Label(
            root,
            text="ZotSearch",
            font=("Arial", 28, "bold"),
            fg="#0064a4"
        )
        header.pack(pady=15)

        # Frame holding the search entry field and buttons.
        search_frame = tk.Frame(root)
        search_frame.pack(pady=10, fill=tk.X, padx=40)

        # Query text is stored in a Tkinter StringVar for easy UI binding.
        self.query_var = tk.StringVar()

        # Main search input box. Pressing Enter triggers a search.
        self.search_entry = tk.Entry(
            search_frame,
            textvariable=self.query_var,
            font=("Arial", 14)
        )
        self.search_entry.pack(side=tk.LEFT, expand=True, fill=tk.X, ipady=5)
        self.search_entry.bind("<Return>", lambda event: self.perform_search())
        self.search_entry.focus_set()

        # Search button submits the current query.
        search_btn = tk.Button(
            search_frame,
            text="Search",
            command=self.perform_search,
            bg="#ffd200",
            font=("Arial", 10, "bold")
        )
        search_btn.pack(side=tk.LEFT, padx=10, ipadx=10, ipady=5)

        # Clear button resets the query field and current results.
        clear_btn = tk.Button(
            search_frame,
            text="Clear",
            command=self.clear_search
        )
        clear_btn.pack(side=tk.LEFT, ipady=5)

        # Label used to display query timing and result count.
        self.info_label = tk.Label(
            root,
            text="",
            font=("Arial", 10, "italic"),
            anchor="w",
            justify="left"
        )
        self.info_label.pack(anchor=tk.W, padx=40)

        # Results area: scrolling list of ranked URLs.
        list_frame = tk.Frame(root)
        list_frame.pack(expand=True, fill=tk.BOTH, padx=40, pady=10)

        scrollbar = tk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.result_list = tk.Listbox(
            list_frame,
            font=("Arial", 12),
            yscrollcommand=scrollbar.set
        )
        self.result_list.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
        scrollbar.config(command=self.result_list.yview)

        # Double-clicking a result opens the URL in the default web browser.
        self.result_list.bind("<Double-1>", self.open_url)

        # Footer reminder for how to interact with results.
        footer = tk.Label(
            root,
            text="Double-click a URL to open in browser",
            font=("Arial", 8),
            fg="gray"
        )
        footer.pack(side=tk.BOTTOM, pady=5)

    def perform_search(self):
        """
        Execute a search using the current query string.

        This method:
        - reads the query from the search box
        - calls the Searcher backend
        - measures runtime
        - updates the results list and info label

        Empty queries are ignored.

        :return: None
        """

        query = self.query_var.get().strip()
        if not query or self.searcher is None:
            return

        # Clear prior results and give the user immediate feedback.
        self.result_list.delete(0, tk.END)
        self.info_label.config(text="Searching...")
        self.root.update_idletasks()

        # Measure only query execution time, not startup time.
        try:
            start_time = time.time()
            results = self.searcher.search(query, top_k=20)
            end_time = time.time()
        except Exception as e:
            self.info_label.config(text="")
            messagebox.showerror("Search Error", f"Could not complete search:\n{e}")
            return

        duration = round((end_time - start_time) * 1000, 2)

        # Update the UI with results or a no-results message.
        if not results:
            self.info_label.config(text=f"No results found for '{query}' ({duration} ms)")
            self.result_list.insert(tk.END, "Try a different search term.")
        else:
            self.info_label.config(text=f"Found {len(results)} results in {duration} ms")
            for url in results:
                self.result_list.insert(tk.END, url)

    def clear_search(self):
        """
        Clear the current query and displayed results.

        The cursor focus is returned to the search entry box so the user
        can immediately type another query.

        :return: None
        """

        self.query_var.set("")
        self.result_list.delete(0, tk.END)
        self.info_label.config(text="")
        self.search_entry.focus_set()

    def open_url(self, event):
        """
        Open the selected search result in the default web browser.

        This method is triggered by double-clicking an entry in the
        results list.

        :param event: Tkinter event object.
        :type event: tk.Event
        :return: None
        """

        selection = self.result_list.curselection()
        if not selection:
            return

        url = self.result_list.get(selection[0])

        # Only attempt to open strings that look like web URLs.
        if isinstance(url, str) and url.startswith("http"):
            webbrowser.open(url)

    def on_close(self):
        """
        Close the application and release search resources.

        This ensures open file handles inside the Searcher backend
        are closed before destroying the Tkinter window.

        :return: None
        """

        try:
            if self.searcher is not None:
                self.searcher.close()
        except Exception:
            pass

        self.root.destroy()


if __name__ == "__main__":
    # Launch the graphical search application.
    root = tk.Tk()
    app = SearchApp(root)
    root.mainloop()

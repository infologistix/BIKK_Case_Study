import numpy as np
import pandas as pd


class transaktion_factory:
    def __init__(self, c_count: int, b_count: int) -> None:
        """        
        Diese Klasse simuliert das Leihverhalten. Zu Beginn
        sind keine Bücher verliehen. Mit der Zeit wird Buch für
        Buch verliehen. Dabei wird der Status in einem Dictionary
        gespeichert. Ist das Buch verfügbar, so wird es verliehen
        und das Buch als key mit dem Wert der Kunden_ID gespeichert.
        Wird das Buch verfügbar, so wird es aus dem Dictionary entfernt.

        Parameters
        ----------
        c_count : int
            Number of customers.
        b_count : int
            Number of books.
        """
        self.c_count = c_count
        self.b_count = b_count
        self.state = {}


    def add_customer(self) -> None:
        self.c_count += 1


    def subtract_customer(self) -> None:
        self.c_count -= 1


    def leihe_buch(self) -> tuple:
        while 1:
            book = np.random.randint(1, self.b_count)
            if book in self.state:
                continue
            else:
                customer = np.random.randint(1, self.c_count)
                self.state[book] = customer
                return (book, customer)


    def rückgabe_buch(self) -> tuple:
        buch = np.random.choice(list(self.state.keys()))
        return (buch, self.state.pop(buch))
        


import psycopg2
import psycopg2.extras
from time import time

class Model:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname='tourist-agency-portal',
            user='postgres',
            password='1234',
            host='localhost',
            port=5432
        )
        self.create_tables()

    def create_tables(self):
        with self.conn.cursor() as c:
            c.execute('''
                CREATE TABLE IF NOT EXISTS Clients (
                    ClientID SERIAL PRIMARY KEY,
                    Name VARCHAR(100) NOT NULL,
                    Email VARCHAR(100) NOT NULL UNIQUE,
                    Phone VARCHAR(15)
                );
                CREATE TABLE IF NOT EXISTS Tours (
                    TourID SERIAL PRIMARY KEY,
                    Name VARCHAR(100) NOT NULL,
                    Country VARCHAR(100) NOT NULL,
                    Price DECIMAL(10, 2) NOT NULL
                );
                CREATE TABLE IF NOT EXISTS Orders (
                    OrderID SERIAL PRIMARY KEY,
                    ClientID INT NOT NULL,
                    TourID INT NOT NULL,
                    OrderDate DATE NOT NULL,
                    Status VARCHAR(50) NOT NULL,
                    PeopleCount INT NOT NULL,
                    Discount DECIMAL(5, 2),
                    FOREIGN KEY (ClientID) REFERENCES Clients(ClientID) ON DELETE CASCADE,
                    FOREIGN KEY (TourID) REFERENCES Tours(TourID) ON DELETE CASCADE
                );
            ''')
            self.conn.commit()

    def add_client(self, name, email, phone):
        try:
            with self.conn.cursor() as c:
                c.execute('INSERT INTO Clients (Name, Email, Phone) VALUES (%s, %s, %s)', (name, email, phone))
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error adding client: {e}")
            self.conn.rollback()

    def get_all_clients(self):
        with self.conn.cursor() as c:
            c.execute('SELECT * FROM Clients')
            return c.fetchall()

    def update_client(self, client_id, name, email, phone):
        try:
            with self.conn.cursor() as c:
                c.execute(
                    'UPDATE Clients SET Name=%s, Email=%s, Phone=%s WHERE ClientID=%s',
                    (name, email, phone, client_id)
                )
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error updating client: {e}")
            self.conn.rollback()

    def delete_client(self, client_id):
        try:
            with self.conn.cursor() as c:
                c.execute('DELETE FROM Clients WHERE ClientID=%s', (client_id,))
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error deleting client: {e}")
            self.conn.rollback()

    def add_tour(self, name, country, price):
        try:
            with self.conn.cursor() as c:
                c.execute('INSERT INTO Tours (Name, Country, Price) VALUES (%s, %s, %s)', (name, country, price))
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error adding tour: {e}")
            self.conn.rollback()

    def get_all_tours(self):
        with self.conn.cursor() as c:
            c.execute('SELECT * FROM Tours')
            return c.fetchall()

    def create_order(self, client_id, tour_id, order_date, status, people_count, discount):
        try:
            with self.conn.cursor() as c:
                c.execute('''
                    INSERT INTO Orders (ClientID, TourID, OrderDate, Status, PeopleCount, Discount)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', (client_id, tour_id, order_date, status, people_count, discount))
                self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error creating order: {e}")
            self.conn.rollback()

    def get_all_orders(self):
        with self.conn.cursor() as c:
            c.execute('''
                SELECT Orders.OrderID, Clients.Name, Tours.Name, Orders.OrderDate, Orders.Status,
                       Orders.PeopleCount, Orders.Discount
                FROM Orders
                JOIN Clients ON Orders.ClientID = Clients.ClientID
                JOIN Tours ON Orders.TourID = Tours.TourID
            ''')
            return c.fetchall()
    
    def generate_random_data(self):
        with self.conn.cursor() as c:
            # Generate random Clients
            c.execute('''
                INSERT INTO Clients (Name, Email, Phone)
                SELECT
                    'Client_' || trunc(random() * 1000)::TEXT,
                    'client_' || trunc(random() * 1000)::TEXT || '@example.com',
                    '+1-' || trunc(random() * 900 + 100)::TEXT || '-' || trunc(random() * 10000)::TEXT
                FROM generate_series(1, 10);
            ''')

            # Generate random Tours
            c.execute('''
                INSERT INTO Tours (Name, Country, Price)
                SELECT
                    'Tour_' || trunc(random() * 100)::TEXT,
                    (ARRAY['USA', 'France', 'Italy', 'Japan', 'Brazil'])[floor(random() * 5 + 1)::INT],
                    (random() * 1000 + 100)::NUMERIC(10, 2)
                FROM generate_series(1, 10);
            ''')

            # Generate random Orders
            c.execute('''
                INSERT INTO Orders (ClientID, TourID, OrderDate, Status, PeopleCount, Discount)
                SELECT
                    (SELECT ClientID FROM Clients ORDER BY random() LIMIT 1),
                    (SELECT TourID FROM Tours ORDER BY random() LIMIT 1),
                    CURRENT_DATE - floor(random() * 30)::INT,
                    (ARRAY['Pending', 'Confirmed', 'Cancelled'])[floor(random() * 3 + 1)::INT],
                    floor(random() * 10 + 1)::INT,
                    (random() * 50)::NUMERIC(5, 2)
                FROM generate_series(1, 10);
            ''')

            self.conn.commit()

    def search_clients_and_orders(self, client_name_pattern, order_status):
        """
        Search for clients and their orders with filters:
        - Client name LIKE pattern
        - Order status = given value
        """
        start_time = time()
        with self.conn.cursor() as c:
            c.execute('''
                SELECT c.Name, c.Email, o.OrderID, o.Status, o.OrderDate
                FROM Clients c
                JOIN Orders o ON c.ClientID = o.ClientID
                WHERE c.Name ILIKE %s AND o.Status = %s
                GROUP BY c.Name, c.Email, o.OrderID, o.Status, o.OrderDate
            ''', (f'%{client_name_pattern}%', order_status))
            results = c.fetchall()
        end_time = time()
        query_time = (end_time - start_time) * 1000
        return results, query_time

    def search_tours_with_price_range(self, min_price, max_price, country_pattern):
        """
        Search for tours with filters:
        - Price between min_price and max_price
        - Country LIKE pattern
        """
        start_time = time()
        with self.conn.cursor() as c:
            c.execute('''
                SELECT t.Name, t.Country, t.Price
                FROM Tours t
                WHERE t.Price BETWEEN %s AND %s AND t.Country ILIKE %s
                GROUP BY t.Name, t.Country, t.Price
            ''', (min_price, max_price, f'%{country_pattern}%'))
            results = c.fetchall()
        end_time = time()
        query_time = (end_time - start_time) * 1000
        return results, query_time

    def search_orders_with_date_range(self, start_date, end_date, min_people, max_people):
        """
        Search for orders with filters:
        - Order date between start_date and end_date
        - People count between min_people and max_people
        """
        start_time = time()
        with self.conn.cursor() as c:
            c.execute('''
                SELECT o.OrderID, c.Name, t.Name, o.OrderDate, o.PeopleCount
                FROM Orders o
                JOIN Clients c ON o.ClientID = c.ClientID
                JOIN Tours t ON o.TourID = t.TourID
                WHERE o.OrderDate BETWEEN %s AND %s
                  AND o.PeopleCount BETWEEN %s AND %s
                GROUP BY o.OrderID, c.Name, t.Name, o.OrderDate, o.PeopleCount
            ''', (start_date, end_date, min_people, max_people))
            results = c.fetchall()
        end_time = time()
        query_time = (end_time - start_time) * 1000
        return results, query_time

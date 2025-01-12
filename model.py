import psycopg2
import psycopg2.extras
from time import time
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

# Define ORM Models
class Client(Base):
    __tablename__ = 'clients'

    clientid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    phone = Column(String(15))
    orders = relationship('Order', back_populates='client', cascade='all, delete-orphan')


class Tour(Base):
    __tablename__ = 'tours'

    tourid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    country = Column(String(100), nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)
    orders = relationship('Order', back_populates='tour', cascade='all, delete-orphan')


class Order(Base):
    __tablename__ = 'orders'

    orderid = Column(Integer, primary_key=True, autoincrement=True)
    clientid = Column(Integer, ForeignKey('clients.clientid', ondelete='CASCADE'), nullable=False)
    tourid = Column(Integer, ForeignKey('tours.tourid', ondelete='CASCADE'), nullable=False)
    orderdate = Column(Date, nullable=False)
    status = Column(String(50), nullable=False)
    peoplecount = Column(Integer, nullable=False)
    discount = Column(DECIMAL(5, 2))

    client = relationship('Client', back_populates='orders')
    tour = relationship('Tour', back_populates='orders')


class Model:
    def __init__(self):
        # psycopg2 connection
        self.conn = psycopg2.connect(
            dbname='tourist-agency-portal',
            user='postgres',
            password='1234',
            host='localhost',
            port=5432
        )

        # SQLAlchemy engine and session
        self.engine = create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/tourist-agency-portal')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        self.create_tables()

    def create_tables(self):
        with self.conn.cursor() as c:
            c.execute('''
                CREATE TABLE IF NOT EXISTS clients (
                    clientid SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    phone VARCHAR(15)
                );
                CREATE TABLE IF NOT EXISTS tours (
                    tourid SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    country VARCHAR(100) NOT NULL,
                    price DECIMAL(10, 2) NOT NULL
                );
                CREATE TABLE IF NOT EXISTS orders (
                    orderid SERIAL PRIMARY KEY,
                    clientid INT NOT NULL,
                    tourid INT NOT NULL,
                    orderdate DATE NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    peoplecount INT NOT NULL,
                    discount DECIMAL(5, 2),
                    FOREIGN KEY (clientid) REFERENCES clients(clientid) ON DELETE CASCADE,
                    FOREIGN KEY (tourid) REFERENCES tours(tourid) ON DELETE CASCADE
                );
            ''')
            self.conn.commit()

    def add_client(self, name, email, phone):
        session = self.Session()
        try:
            client = Client(name=name, email=email, phone=phone)
            session.add(client)
            session.commit()
        except Exception as e:
            print(f"Error adding client: {e}")
            session.rollback()
        finally:
            session.close()

    def get_all_clients(self):
        session = self.Session()
        try:
            return [(client.clientid, client.name, client.email, client.phone) for client in session.query(Client).all()]
        finally:
            session.close()

    def update_client(self, client_id, name, email, phone):
        session = self.Session()
        try:
            client = session.query(Client).filter_by(clientid=client_id).first()
            if client:
                client.name = name
                client.email = email
                client.phone = phone
                session.commit()
        except Exception as e:
            print(f"Error updating client: {e}")
            session.rollback()
        finally:
            session.close()

    def delete_client(self, client_id):
        session = self.Session()
        try:
            client = session.query(Client).filter_by(clientid=client_id).first()
            if client:
                session.delete(client)
                session.commit()
        except Exception as e:
            print(f"Error deleting client: {e}")
            session.rollback()
        finally:
            session.close()

    def add_tour(self, name, country, price):
        session = self.Session()
        try:
            tour = Tour(name=name, country=country, price=price)
            session.add(tour)
            session.commit()
        except Exception as e:
            print(f"Error adding tour: {e}")
            session.rollback()
        finally:
            session.close()

    def get_all_tours(self):
        session = self.Session()
        try:
            return [(tour.tourid, tour.name, tour.country, tour.price) for tour in session.query(Tour).all()]
        finally:
            session.close()

    def create_order(self, client_id, tour_id, order_date, status, people_count, discount):
        session = self.Session()
        try:
            order = Order(
                clientid=client_id,
                tourid=tour_id,
                orderdate=order_date,
                status=status,
                peoplecount=people_count,
                discount=discount
            )
            session.add(order)
            session.commit()
        except Exception as e:
            print(f"Error creating order: {e}")
            session.rollback()
        finally:
            session.close()

    def get_all_orders(self):
        session = self.Session()
        try:
            return [(order.orderid, order.client.name, order.tour.name, order.orderdate, order.status,
                     order.peoplecount, order.discount) for order in session.query(Order).all()]
        finally:
            session.close()
    
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

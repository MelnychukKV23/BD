class View:
    def show_clients(self, clients):
        print("Clients:")
        for client in clients:
            print(f"ID: {client[0]}, Name: {client[1]}, Email: {client[2]}, Phone: {client[3]}")

    def get_client_input(self):
        name = input("Enter client name: ")
        email = input("Enter client email: ")
        phone = input("Enter client phone: ")
        return name, email, phone

    def get_client_id(self):
        return int(input("Enter client ID: "))

    def show_tours(self, tours):
        print("Tours:")
        for tour in tours:
            print(f"ID: {tour[0]}, Name: {tour[1]}, Country: {tour[2]}, Price: {tour[3]}")

    def get_tour_input(self):
        name = input("Enter tour name: ")
        country = input("Enter tour country: ")
        price = float(input("Enter tour price: "))
        return name, country, price

    def get_order_input(self):
        client_id = int(input("Enter client ID: "))
        tour_id = int(input("Enter tour ID: "))
        order_date = input("Enter order date (YYYY-MM-DD): ")
        status = input("Enter order status: ")
        people_count = int(input("Enter number of people: "))
        discount = float(input("Enter discount: "))
        return client_id, tour_id, order_date, status, people_count, discount

    def show_orders(self, orders):
        print("Orders:")
        for order in orders:
            print(f"OrderID: {order[0]}, Client: {order[1]}, Tour: {order[2]}, Date: {order[3]}, "
                  f"Status: {order[4]}, PeopleCount: {order[5]}, Discount: {order[6]}")

    def show_message(self, message):
        print(message)
    
    def show_search_results(self, results, query_time):
        print("Search Results:")
        for row in results:
            print(row)
        print(f"Query executed in {query_time:.2f} ms.")

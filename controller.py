from model import Model
from view import View


from model import Model
from view import View


class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.show_menu()
            if choice == '1':
                self.add_client()
            elif choice == '2':
                self.view_clients()
            elif choice == '3':
                self.update_client()
            elif choice == '4':
                self.delete_client()
            elif choice == '5':
                self.add_tour()
            elif choice == '6':
                self.view_tours()
            elif choice == '7':
                self.create_order()
            elif choice == '8':
                self.view_orders()
            elif choice == '9':
                self.generate_data()
            elif choice == '10':
                self.search_clients_and_orders()
            elif choice == '11':
                self.search_tours_with_price_range()
            elif choice == '12':
                self.search_orders_with_date_range()
            else:
                break

    def show_menu(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add Client")
        self.view.show_message("2. View Clients")
        self.view.show_message("3. Update Client")
        self.view.show_message("4. Delete Client")
        self.view.show_message("5. Add Tour")
        self.view.show_message("6. View Tours")
        self.view.show_message("7. Create Order")
        self.view.show_message("8. View Orders")
        self.view.show_message("9. Generate Random Data")
        self.view.show_message("10. Search Clients and Orders")
        self.view.show_message("11. Search Tours with Price Range")
        self.view.show_message("12. Search Orders with Date Range")
        self.view.show_message("13. Exit")
        return input("Enter your choice: ")

    def add_client(self):
        name, email, phone = self.view.get_client_input()
        self.model.add_client(name, email, phone)
        self.view.show_message("Client added successfully!")

    def view_clients(self):
        clients = self.model.get_all_clients()
        self.view.show_clients(clients)

    def update_client(self):
        client_id = self.view.get_client_id()
        name, email, phone = self.view.get_client_input()
        self.model.update_client(client_id, name, email, phone)
        self.view.show_message("Client updated successfully!")

    def delete_client(self):
        client_id = self.view.get_client_id()
        self.model.delete_client(client_id)
        self.view.show_message("Client deleted successfully!")

    def add_tour(self):
        name, country, price = self.view.get_tour_input()
        self.model.add_tour(name, country, price)
        self.view.show_message("Tour added successfully!")

    def view_tours(self):
        tours = self.model.get_all_tours()
        self.view.show_tours(tours)

    def create_order(self):
        client_id, tour_id, order_date, status, people_count, discount = self.view.get_order_input()
        self.model.create_order(client_id, tour_id, order_date, status, people_count, discount)
        self.view.show_message("Order created successfully!")

    def view_orders(self):
        orders = self.model.get_all_orders()
        self.view.show_orders(orders)

    def generate_data(self):
        self.model.generate_random_data()
        self.view.show_message("Random data generated successfully!")

    def search_clients_and_orders(self):
        name_pattern = input("Enter client name pattern: ")
        status = input("Enter order status: ")
        results, query_time = self.model.search_clients_and_orders(name_pattern, status)
        self.view.show_search_results(results, query_time)

    def search_tours_with_price_range(self):
        min_price = float(input("Enter minimum price: "))
        max_price = float(input("Enter maximum price: "))
        country_pattern = input("Enter country pattern: ")
        results, query_time = self.model.search_tours_with_price_range(min_price, max_price, country_pattern)
        self.view.show_search_results(results, query_time)

    def search_orders_with_date_range(self):
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        min_people = int(input("Enter minimum people count: "))
        max_people = int(input("Enter maximum people count: "))
        results, query_time = self.model.search_orders_with_date_range(start_date, end_date, min_people, max_people)
        self.view.show_search_results(results, query_time)

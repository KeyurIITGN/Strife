import grpc
import logging
import uuid
import hashlib
import json
import os
import sys
from collections import deque
import time
import threading

import protofiles.payment_pb2 as payment_pb2
import protofiles.payment_pb2_grpc as payment_pb2_grpc

class PaymentClient:
    
    def __init__(self):
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - Client - %(levelname)s - %(message)s'
        )
        
        # Initialize empty channel and stub
        self.channel = None
        self.stub = None
        self.token = None
        self.is_connected = False

        self.stop_monitoring = False
        self.connectivity_thread = None

        self.client_id = str(uuid.uuid4())
        self.pending_dir = f"pending_transactions/{self.client_id}"
        os.makedirs(self.pending_dir, exist_ok=True)

    def start_connectivity_monitor(self, check_interval = 10):
        if self.connectivity_thread and self.connectivity_thread.is_alive():
            #logging.info("Connectivity monitor running...")
            return
        
        self.stop_monitoring = False    
        self.connectivity_thread = threading.Thread(
            target = self._connectivity_monitor_loop,
            args = (check_interval,),
            daemon = True
        )
        self.connectivity_thread.start()
        #logging.info(f"Starting connectivity monitor (checking every {check_interval} seconds)")

    def stop_connectivity_monitor(self):
        """Stop the connectivity monitoring thread"""
        self.stop_monitoring = True
        if self.connectivity_thread:
            self.connectivity_thread.join(timeout=1.0)  # Wait for thread to finish
            logging.info("Stopped connectivity monitor")

    def _connectivity_monitor_loop(self, check_interval):
        """Background loop that periodically checks connectivity and retries transactions"""
        while not self.stop_monitoring:
            reconnect_needed = False
            
            # Check if we need to reconnect (either not connected or server might be gone)
            if not self.is_connected:
                reconnect_needed = True
            else:
                # Test the existing connection
                try:
                    # A simple health check - try to use the stub
                    # This will throw an exception if the server is gone
                    metadata = [('test', 'test')]
                    self.stub.CheckBalance(payment_pb2.BalanceRequest(), metadata=metadata, timeout=2)
                except:
                    # If any error occurs, we need to reconnect
                    #logging.info("Existing connection seems broken, reconnecting...")
                    self.is_connected = False
                    reconnect_needed = True
                    
            # Try to reconnect if needed
            if reconnect_needed:
                #logging.info("Connectivity monitor attempting to reconnect...")
                # Close existing channel first if there is one
                if self.channel:
                    try:
                        self.channel.close()
                    except:
                        pass
                # Try to reconnect
                self.connect()
                
            # If we're connected and authenticated, retry pending transactions
            if self.is_connected and self.token and self.has_pending_transactions():
                #logging.info("Connectivity monitor retrying pending transactions...")
                self.retry_pending_transactions()
                
            # Sleep for the specified interval
            time.sleep(check_interval)

    def has_pending_transactions(self):
        """Check if this client has any pending transactions"""
        if not os.path.exists(self.pending_dir):
            return False
        
        return any(f.endswith('.json') for f in os.listdir(self.pending_dir))



    
    def connect(self, server_address='localhost:50051'):
        """Connect to the payment gateway server with SSL/TLS"""
        try:
            # Load client key and certificate
            with open('certificate/client.key', 'rb') as f:
                client_key = f.read()
            with open('certificate/client.cert', 'rb') as f:
                client_cert = f.read()
            
            # Load CA certificate for server validation
            with open('certificate/ca.cert', 'rb') as f:
                ca_cert = f.read()
            
            # Create SSL credentials
            credentials = grpc.ssl_channel_credentials(
                root_certificates=ca_cert,
                private_key=client_key,
                certificate_chain=client_cert
            )
            
            # Create secure channel
            self.channel = grpc.secure_channel(server_address, credentials)
            
            # Create a stub (client)
            self.stub = payment_pb2_grpc.PaymentGatewayStub(self.channel)
            
            self.is_connected = True
            #logging.info(f"Connected to payment gateway at {server_address}")

            if self.is_connected:
                self.start_connectivity_monitor()

            return self.is_connected
        
        except Exception as e:
            logging.error(f"Failed to connect: {str(e)}")
            self.is_connected = False
            return False
    def make_payment(self, receiver_account, receiver_bank, amount, use_fixed_id = False, fixed_id = None):
        """Initiate a payment with idempotency key"""
        if not self.is_connected or not self.token:
            logging.error("Not connected or not authenticated")
            return False, "Not connected or not authenticated"
        
        # Generate a unique payment_id for idempotency (or keep it fixed (for testing))
        if use_fixed_id and fixed_id:
            payment_id = fixed_id
            logging.info("Used fixed payment")
        else:
            payment_id = str(uuid.uuid4())
        
        # Save transaction in pending queue with payment_id
        self._add_to_pending_transactions(payment_id, {
            "receiver_account": receiver_account,
            "receiver_bank": receiver_bank,
            "amount": amount,
            "timestamp": time.time()
        })
        
        # Attempt to send the payment
        success, message = self._send_payment(payment_id, receiver_account, receiver_bank, amount)
        
        # If successful, remove from pending queue
        if success:
            self._remove_from_pending_transactions(payment_id)
            return success, message, payment_id
        else:
            if "UNAVAILABLE" in message:
                return False, "Payment server is currently unavailable. Will try later", payment_id
            else:
                return False , message, payment_id

    def _send_payment(self, payment_id, receiver_account, receiver_bank, amount):
        """Send a payment request to the gateway"""
        try:
            # Create payment request with payment_id for idempotency
            request = payment_pb2.PaymentRequest(
                token=self.token,  # Token is included in the request per your proto
                sender_account="self",  # Use the authenticated account
                receiver_account=receiver_account,
                receiver_bank=receiver_bank,
                amount=amount,
                payment_id=payment_id  # Use payment_id for idempotency
            )


            metadata = [('token', self.token)]
            
            # Call the payment service (no metadata needed as token is in request)
            response = self.stub.ProcessPayment(request, metadata=metadata)
            
            if response.success:
                logging.info(f"Payment successful: {response.message}")
                return True, response.message
            else:
                logging.error(f"Payment failed: {response.message}")
                return False, response.message
            
        except grpc.RpcError as e:
            error_message = f"RPC error during payment: {e.code().name}"
            logging.error(error_message)
            
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logging.info("Server unavailable, payment will be retried later")
            
            return False, error_message
        
    def _add_to_pending_transactions(self, idempotency_key, transaction):
        """Add transaction to pending queue"""
        # Ensure pending transactions directory exists
        os.makedirs(self.pending_dir, exist_ok=True)
        
        # Save transaction details to file with idempotency key as filename
        file_path = f"{self.pending_dir}/{idempotency_key}.json"
        with open(file_path, 'w') as f:
            json.dump(transaction, f)
        
        logging.info(f"Added transaction to pending queue: {idempotency_key}")

    def _remove_from_pending_transactions(self, idempotency_key):
        """Remove transaction from pending queue"""
        file_path = f"{self.pending_dir}/{idempotency_key}.json"
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Removed transaction from pending queue: {idempotency_key}")  
      
    def retry_pending_transactions(self):
        """Retry all pending transactions for this client"""
        if not self.is_connected or not self.token:
            logging.error("Not connected or not authenticated")
            return False, "Not connected or not authenticated"
        
        # Get list of pending transaction files for this client only
        if not os.path.exists(self.pending_dir):
            return True, "No pending transactions"
        
        pending_files = os.listdir(self.pending_dir)
        success = True
        retried_count = 0
        success_count = 0
        
        for file_name in pending_files:
            if not file_name.endswith('.json'):
                continue
            
            idempotency_key = file_name[:-5]  # Remove .json extension
            file_path = f"{self.pending_dir}/{file_name}"
            
            # Load transaction details
            with open(file_path, 'r') as f:
                try:
                    transaction = json.load(f)
                except json.JSONDecodeError:
                    logging.error(f"Error reading pending transaction: {file_path}")
                    continue
            
            # Retry sending the payment
            logging.info(f"Retrying pending transaction: {idempotency_key}")
            retried_count += 1
            
            retry_success, message = self._send_payment(
                idempotency_key,
                transaction["receiver_account"],
                transaction["receiver_bank"],
                transaction["amount"]
            )
            
            if retry_success:
                success_count += 1
                self._remove_from_pending_transactions(idempotency_key)
            else:
                success = False
        
        result_message = f"Retried {retried_count} transactions, {success_count} succeeded"
        return success, result_message
    



    def authenticate(self, username, password, bank_name):
        """Authenticate with the payment gateway"""
        if not self.is_connected:
            logging.error("Not connected to the server")
            return False, "Not connected to the server"
        
        try:
            # Create authentication request
            request = payment_pb2.AuthRequest(
                username=username,
                password=password,
                bank_name=bank_name
            )
            
            # Call the authentication service
            response = self.stub.Authenticate(request)
            
            if response.success:
                self.token = response.token
                logging.info("Authentication successful")

                self.retry_pending_transactions()
                return True, "Authentication successful"
                

                
            else:
                logging.error(f"Authentication failed: {response.message}")
                return False, response.message
        
        except grpc.RpcError as e:
            error_message = f"RPC error during authentication: {e.code().name} - {e.details()}"
            logging.error(error_message)
            return False, error_message
    def check_balance(self):
        """Check the account balance"""
        if not self.is_connected:
            logging.error("Not connected to the server")
            return None, "Not connected to the server"
        
        if not self.token:
            logging.error("Not authenticated")
            return None, "Not authenticated"
        
        try:
            # Create balance request with token in the request
            request = payment_pb2.BalanceRequest(token=self.token)
            
            # Create metadata with token as well - for authorization
            metadata = [('token', self.token)]
            
            # Call the balance service with both the request and metadata
            response = self.stub.CheckBalance(request, metadata=metadata)
            
            if response.success:
                logging.info(f"Balance: {response.balance}")
                return response.balance, "Balance Retrieved Successfully!!"
            else:
                logging.error(f"Failed to get balance: {response.message}")
                return None, response.message
        
        except grpc.RpcError as e:
            error_message = ""
            if e.code() == grpc.StatusCode.PERMISSION_DENIED:
                error_message = f"Authorization error: {e.details()}"
                logging.error(error_message)
            else:
                error_message = f"RPC error during balance check: {e.code().name}"
                logging.error(error_message)
            return None, error_message
    
    def close(self):
        """Close the connection to the server"""
        self.stop_connectivity_monitor()
    
        if self.channel:
            self.channel.close()
            self.is_connected = False
            self.token = None
            logging.info("Connection closed")
            return True
        return False


    def get_pending_transactions(self):
        """Get a list of pending transactions for this client"""
        if not os.path.exists(self.pending_dir):
            return []
        
        pending_transactions = []
        for file_name in os.listdir(self.pending_dir):
            if not file_name.endswith('.json'):
                continue
                
            idempotency_key = file_name[:-5]  # Remove .json extension
            file_path = f"{self.pending_dir}/{file_name}"
            
            try:
                with open(file_path, 'r') as f:
                    transaction = json.load(f)
                    transaction['payment_id'] = idempotency_key
                    pending_transactions.append(transaction)
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Error reading pending transaction {file_path}: {str(e)}")
        
        return pending_transactions
    

def print_menu():
    """Print the main menu options"""
    print("\n====== Strife Payment System ======")
    print("1. Connect to server")
    print("2. Authenticate")
    print("3. Check balance")
    print("4. Make payment")
    print("5. Test idempotency (same payment twice)")
    print("6. View pending transactions")
    print("7. Retry pending transactions")
    print("8. Disconnect")
    print("9. Exit")
    print("===================================")


def run_client_menu():
    """Run the client with menu-driven interface"""
    client = PaymentClient()
    last_payment_id = None
    
    print("Welcome to the Strife Payment System")
    print("-----------------------------------")
    
    while True:
        print_menu()
        choice = input("Enter your choice (1-9): ")
        
        if choice == '1':
            # Connect to server
            if client.is_connected:
                print("Already connected to the server")
                continue
                
            server_address = input("Enter server address (default: localhost:50051): ")
            if not server_address:
                server_address = "localhost:50051"
            
            print(f"Connecting to {server_address}...")
            if client.connect(server_address):
                print("Successfully connected to payment gateway")
            else:
                print("Connection failed")
                
        elif choice == '2':
            # Authenticate
            if not client.is_connected:
                print("Not connected to server. Please connect first")
                continue
                
            if client.token:
                print(f"Already authenticated with token: {client.token}")
                cont = input("Do you want to re-authenticate? (y/n): ")
                if cont.lower() != 'y':
                    continue
            
            username = input("Username: ")
            password = input("Password: ")
            bank_name = input("Bank name: ")
            
            print("Authenticating...")
            success, message = client.authenticate(username, password, bank_name)
            
            if success:
                print(f"Authentication successful. Token: {client.token}")
            else:
                print(f"Authentication failed: {message}")
                
        elif choice == '3':
            # Check balance
            if not client.is_connected or not client.token:
                print("Not connected or not authenticated")
                continue
                
            print("Checking account balance...")
            balance, message = client.check_balance()
            
            if balance is not None:
                print(f"Your current balance: {balance}")
            else:
                print(f"Failed to retrieve balance: {message}")
                
        elif choice == '4':
            # Make payment
            if not client.is_connected or not client.token:
                print("Not connected or not authenticated")
                continue
                
            receiver_account = input("Enter receiver account ID: ")
            receiver_bank = input("Enter receiver bank name: ")
            
            try:
                amount = float(input("Enter amount to send: "))
            except ValueError:
                print("Invalid amount. Please enter a number")
                continue
                
            print("Processing payment...")
            success, message, payment_id = client.make_payment(receiver_account, receiver_bank, amount)
            last_payment_id = payment_id
            
            if success:
                print(f"Payment successful! Message: {message}")
                print(f"Payment ID: {payment_id} (save this for idempotency testing)")
            else:
                print(f"Payment failed: {message}")
                print(f"Transaction saved to pending queue with ID: {payment_id}")
                
        elif choice == '5':
            # Test idempotency
            if not client.is_connected or not client.token:
                print("Not connected or not authenticated")
                continue
                
            print("\n--- Idempotency Test ---")
            print("This will send the same payment twice to test idempotency")
            
            # Get payment details
            use_last = False
            if last_payment_id:
                use_last = input(f"Use last payment ID ({last_payment_id})? (y/n): ").lower() == 'y'
            
            if use_last:
                payment_id = last_payment_id
            else:
                payment_id = input("Enter payment ID to reuse: ")
                if not payment_id:
                    print("Payment ID is required for idempotency testing")
                    continue
            
            receiver_account = input("Enter receiver account ID: ")
            receiver_bank = input("Enter receiver bank name: ")
            
            try:
                amount = float(input("Enter amount to send: "))
            except ValueError:
                print("Invalid amount. Please enter a number")
                continue
            
            # First payment 
            print("\nSending first payment...")
            success1, message1, _ = client.make_payment(
                receiver_account, receiver_bank, amount, 
                use_fixed_id=True, fixed_id=payment_id
            )
            
            if success1:
                print(f"First payment successful: {message1}")
            else:
                print(f"First payment failed: {message1}")
            
            # Second identical payment with same ID
            input("\nPress Enter to send identical payment with same ID...")
            print("Sending second payment with same payment_id...")
            success2, message2, _ = client.make_payment(
                receiver_account, receiver_bank, amount,
                use_fixed_id=True, fixed_id=payment_id
            )
            
            if success2:
                print(f"Second payment successful: {message2}")
            else:
                print(f"Second payment failed: {message2}")
                
            print("\nIdempotency test complete. Check the gateway logs to verify if")
            print("the second request was processed or returned from cache.")
            
        elif choice == '6':
            # View pending transactions
            pending = client.get_pending_transactions()
            
            if not pending:
                print("No pending transactions found")
            else:
                print("\n--- Pending Transactions ---")
                print(f"Total: {len(pending)} pending transactions")
                
                for i, tx in enumerate(pending, 1):
                    print(f"\n{i}. Payment ID: {tx['payment_id']}")
                    print(f"   Receiver: {tx['receiver_account']} at {tx['receiver_bank']}")
                    print(f"   Amount: {tx['amount']}")
                    print(f"   Timestamp: {time.ctime(tx['timestamp'])}")
                
        elif choice == '7':
            # Retry pending transactions
            if not client.is_connected or not client.token:
                print("Not connected or not authenticated")
                continue
                
            print("Retrying pending transactions...")
            success, message = client.retry_pending_transactions()
            
            print(message)
            
        elif choice == '8':
            # Disconnect
            if not client.is_connected:
                print("Not connected to server")
                continue
                
            if client.close():
                print("Disconnected from server")
            else:
                print("Failed to disconnect")
                
        elif choice == '9':
            # Exit
            print("Exiting Strife Payment System. Goodbye!")
            if client.is_connected:
                client.close()
            break
            
        else:
            print("Invalid choice. Please enter a number between 1 and 9")
        
        input("\nPress Enter to continue...")

def run_client():
    """Run the client with user interaction"""
    client = PaymentClient()
    
    print("Welcome to the Strife Payment System")
    print("-----------------------------------")
    
    # Get server address
    server_address = input("Enter server address (default: localhost:50051): ")
    if not server_address:
        server_address = "localhost:50051"
    
    # Connect to server
    print(f"Connecting to {server_address}...")
    if not client.connect(server_address):
        print("Connection failed. Exiting.")
        return
    
    print("Connected to payment gateway.")
    
    # Get authentication details
    username = input("Username: ")
    password = input("Password: ")
    bank_name = input("Bank name: ")
    
    # Authenticate
    print("Authenticating...")
    success, message = client.authenticate(username, password, bank_name)
    
    if success:
        print(f"Authentication successful. Token: {client.token}")
        print("\nChecking account balance...")
        balance, message = client.check_balance()
        
        if balance is not None:
            print(f"Your current balance: {balance}")
        else:
            print(f"Failed to retrieve balance: {message}")        
    else:
        print(f"Authentication failed: {message}")
    
    # Clean up
    client.close()
    print("Connection closed.")

if __name__ == "__main__":
    run_client_menu()
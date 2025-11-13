import grpc
from concurrent import futures
import json
import logging
import os
import sys
import time
import uuid 
# Import the generated protocol buffer code
import protofiles.payment_pb2 as payment_pb2
import protofiles.payment_pb2_grpc as payment_pb2_grpc

class BankServicer(payment_pb2_grpc.BankServiceServicer):
    """Implementation of the Bank service for authentication"""
    
    def __init__(self, bank_name):
        self.bank_name = bank_name
        
        # Load user credentials from a JSON file
        self.users = self._load_user_credentials()
        self.transactions = self._load_transactions()
        self.processed_transactions = self._load_processed_transactions()

        self.prepared_transactions = {}
        
        logging.info(f"Bank {bank_name} initialized with {len(self.users)} users")

        

    def _load_processed_transactions(self):
        """Load record of processed transactions"""
        file_path = f"data/{self.bank_name.lower()}_processed_transactions.json"
        
        if not os.path.exists(file_path):
            # Initialize empty set of processed transactions
            with open(file_path, 'w') as f:
                json.dump({}, f)
            
            return {}
        
        # Load from file
        with open(file_path, 'r') as f:
            return json.load(f)
        
    def _save_processed_transactions(self):
        """Save processed transactions to a file"""
        file_path = f"data/{self.bank_name.lower()}_processed_transactions.json"
        
        with open(file_path, 'w') as f:
            json.dump(self.processed_transactions, f, indent=2)

    def ProcessTransaction(self, request, context):
        """Process a debit or credit transaction with idempotency support"""
        account_id = request.account_id
        amount = request.amount
        transaction_type = request.type
        payment_id = request.payment_id
        counterparty = request.counterparty
        
        # Log transaction request
        logging.info(f"Transaction request: account={account_id}, type={transaction_type}, amount={amount}")
        
        # Idempotency check - if we've processed this transaction before, return the cached result
        if payment_id and payment_id in self.processed_transactions:
            logging.info(f"Returning cached result for idempotent transaction: {payment_id}")
            return payment_pb2.BankTransactionResponse(**self.processed_transactions[payment_id])
        
        # Validate account
        account_exists = False
        for username, user_data in self.users.items():
            if user_data["account_id"] == account_id:
                account_exists = True
                
                # Process transaction
                if transaction_type == "debit":
                    # Check if account has sufficient funds
                    if user_data["balance"] < amount:
                        response = payment_pb2.BankTransactionResponse(
                            success=False,
                            message=f"Insufficient funds. Current balance: {user_data['balance']}"
                        )
                    else:
                        # Deduct amount from account
                        user_data["balance"] -= amount

                        self._save_user_credentials()
                        
                        # Record transaction
                        self.record_transaction(account_id, "debit", amount, counterparty)
                        
                        response = payment_pb2.BankTransactionResponse(
                            success=True,
                            message=f"Debit successful. New balance: {user_data['balance']}"
                        )
                elif transaction_type == "credit":
                    # Add amount to account
                    user_data["balance"] += amount


                    self._save_user_credentials()
                    
                    # Record transaction
                    self.record_transaction(account_id, "credit", amount, counterparty)
                    
                    response = payment_pb2.BankTransactionResponse(
                        success=True,
                        message=f"Credit successful. New balance: {user_data['balance']}"
                    )
                else:
                    response = payment_pb2.BankTransactionResponse(
                        success=False,
                        message=f"Invalid transaction type: {transaction_type}"
                    )
                
                # Cache the result for idempotency
                if payment_id:
                    self.processed_transactions[payment_id] = {
                        "success": response.success,
                        "message": response.message
                    }
                    self._save_processed_transactions()
                
                return response
        
        # If account not found
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Account {account_id} not found in bank {self.bank_name}")
        
        response = payment_pb2.BankTransactionResponse(
            success=False,
            message=f"Account {account_id} not found"
        )
        
        # Cache the result for idempotency
        if payment_id:
            self.processed_transactions[payment_id] = {
                "success": response.success,
                "message": response.message
            }
            self._save_processed_transactions()
        
        return response
    
    def _load_user_credentials(self):
        """Load user credentials from a file"""
        # File path based on bank name
        file_path = f"data/{self.bank_name.lower()}_users.json"
        
        # Check if directory exists, if not create it
        os.makedirs("data", exist_ok=True)
        
        # Check if file exists, if not create sample data
        if not os.path.exists(file_path):
            # Create sample users
            sample_users = {
                f"user{i}": {
                    "password": f"pass{i}",
                    "account_id": f"ACC{i:03d}",
                    "name": f"User {i}",
                    "balance": 1000.0 * i
                } for i in range(1, 6)
            }
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(sample_users, f, indent=2)
            
            logging.info(f"Created sample user data for {self.bank_name}")
        
        # Load users from file
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def VerifyCredentials(self, request, context):
        """Verify user credentials"""
        username = request.username
        password = request.password
        
        # Log verification attempt (omitting password for security)
        logging.info(f"Credential verification attempt: username={username}")
        
        if username in self.users and self.users[username]["password"] == password:
            # Credentials are valid
            account_id = self.users[username]["account_id"]
            logging.info(f"Credential verification successful: username={username}")
            
            return payment_pb2.CredentialVerificationResponse(
                valid=True,
                account_id=account_id,
                message="Credentials verified successfully"
            )
        else:
            # Credentials are invalid
            logging.warning(f"Credential verification failed: username={username}")
            
            return payment_pb2.CredentialVerificationResponse(
                valid=False,
                account_id="",
                message="Invalid username or password"
            )
    def GetBalance(self, request, context):
        """Get the balance of an account"""
        account_id = request.account_id
        
        # Log the balance check
        logging.info(f"Balance check for account {account_id}")
        
        # Check if account exists in this bank
        for username, user_data in self.users.items():
            if user_data["account_id"] == account_id:
                return payment_pb2.BankBalanceResponse(
                    success=True,
                    balance=user_data["balance"],
                    message="Balance retrieved successfully"
                )
        
        # If account not found
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Account {account_id} not found in bank {self.bank_name}")
        return payment_pb2.BankBalanceResponse(
            success=False,
            balance=0,
            message=f"Account {account_id} not found"
        )
    def _load_transactions(self):
        """Load transaction history from a file or initialize if it doesn't exist"""
        file_path = f"data/{self.bank_name.lower()}_transactions.json"
        
        if not os.path.exists(file_path):
            # Initialize with empty transaction history for each account
            transactions = {}
            
            # For each user, create an empty transaction list
            for username, user_data in self.users.items():
                account_id = user_data["account_id"]
                transactions[account_id] = []
                
                # Add some sample transactions for testing
                initial_deposit = {
                    "transaction_id": str(uuid.uuid4()),
                    "type": "credit",
                    "amount": user_data["balance"],
                    "counterparty": "Bank",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "completed"
                }
                transactions[account_id].append(initial_deposit)
            
            # Save to file
            with open(file_path, 'w') as f:
                json.dump(transactions, f, indent=2)
            
            logging.info(f"Initialized transaction history for {self.bank_name}")
            return transactions
        
        # Load transactions from file
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def _save_transactions(self):
        """Save transaction history to a file"""
        file_path = f"data/{self.bank_name.lower()}_transactions.json"
        
        with open(file_path, 'w') as f:
            json.dump(self.transactions, f, indent=2)
    def record_transaction(self, account_id, transaction_type, amount, counterparty, status="completed", transaction_id = None):
        """Record a new transaction in the history"""
        if not hasattr(self, 'transactions'):
            self.transactions = self._load_transactions()

        if account_id not in self.transactions:
            self.transactions[account_id] = []
        
        transaction = {
            "transaction_id": transaction_id if transaction_id else str(uuid.uuid4()),
            "type": transaction_type,
            "amount": amount,
            "counterparty": counterparty,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status
        }
        
        self.transactions[account_id].append(transaction)
        self._save_transactions()
        
        return transaction
    def GetTransactionHistory(self, request, context):
        """Get transaction history for an account"""
        account_id = request.account_id
        limit = request.limit
        
        # Log the history request
        logging.info(f"Transaction history request for account {account_id}, limit {limit}")
        
        # Check if account exists in this bank
        account_exists = False
        for username, user_data in self.users.items():
            if user_data["account_id"] == account_id:
                account_exists = True
                break
        
        if not account_exists:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Account {account_id} not found in bank {self.bank_name}")
            return payment_pb2.BankHistoryResponse(
                success=False,
                message=f"Account {account_id} not found"
            )
        
        # Get transactions for the account
        if account_id not in self.transactions:
            # No transactions found
            return payment_pb2.BankHistoryResponse(
                success=True,
                message="No transactions found for this account"
            )
        
        # Get the most recent transactions up to the limit
        account_transactions = self.transactions[account_id]
        recent_transactions = sorted(
            account_transactions, 
            key=lambda t: t["timestamp"], 
            reverse=True
        )[:limit]
        
        # Convert to protobuf Transaction objects
        pb_transactions = []
        for tx in recent_transactions:
            pb_tx = payment_pb2.Transaction(
                transaction_id=tx["transaction_id"],
                type=tx["type"],
                amount=float(tx["amount"]),
                counterparty=tx["counterparty"],
                timestamp=tx["timestamp"],
                status=tx["status"]
            )
            pb_transactions.append(pb_tx)
        
        return payment_pb2.BankHistoryResponse(
            success=True,
            transactions=pb_transactions,
            message=f"Retrieved {len(pb_transactions)} transactions"
        )

    def _save_user_credentials(self):
        """Save user credentials to the file"""
        file_path = f"data/{self.bank_name.lower()}_users.json"
        with open(file_path, 'w') as f:
            json.dump(self.users, f, indent=2)
        logging.info(f"User data saved for {self.bank_name}")

    def PrepareTransaction(self, request, context):
        transaction_id = request.transaction_id
        account_id = request.account_id
        amount = request.amount
        transaction_type = request.type
        counterparty = request.counterparty

        logging.info(f"Prepare transaction request: id={transaction_id}, account={account_id}, type={transaction_type}, amount={amount}")

        if transaction_id in self.prepared_transactions:
            logging.info(f"Transaction already prepared: {transaction_id}")
            return payment_pb2.PrepareTransactionResponse(
                ready=self.prepared_transactions[transaction_id]["ready"],
                message=self.prepared_transactions[transaction_id]["message"]
            )

        account_exists = False
        account_username = None

        for username, user_data in self.users.items():
            if user_data["account_id"] == account_id:
                account_exists = True
                account_username = username
                break

        if not account_exists:
            logging.warning(f"Account {account_id} not found for prepare transaction")
            return payment_pb2.PrepareTransactionResponse(
                ready=False,
                message=f"Account {account_id} not found"
            )

        if transaction_type == "debit":
            balance = self.users[account_username]["balance"]
            if balance < amount:
                logging.warning(f"Insufficient funds for transaction {transaction_id}: balance={balance}, amount={amount}")
                return payment_pb2.PrepareTransactionResponse(
                    ready=False,
                    message=f"Insufficient funds. Current balance: {balance}, required: {amount}"
                )

        # Store the prepared transaction information
        self.prepared_transactions[transaction_id] = {
            "ready": True,
            "message": "Ready to process transaction",
            "details": {
                "account_id": account_id,
                "username": account_username,
                "type": transaction_type,
                "amount": amount,
                "counterparty": counterparty,
                "timestamp": time.time()
            }
        }

        logging.info(f"Transaction {transaction_id} prepared successfully")
        return payment_pb2.PrepareTransactionResponse(
            ready=True,
            message="Ready to process transaction"
        )
    
    def CommitTransaction(self, request, context):
        """Phase 2 of 2PC: Commit the prepared transaction"""
        transaction_id = request.transaction_id
        
        logging.info(f"Commit transaction request: id={transaction_id}")
        
        # Check if this transaction was prepared
        if transaction_id not in self.prepared_transactions:
            logging.warning(f"Cannot commit unprepared transaction: {transaction_id}")
            return payment_pb2.CommitTransactionResponse(
                success=False,
                message="Transaction not prepared"
            )
        
        # Get the prepared transaction details
        tx_info = self.prepared_transactions[transaction_id]
        
        if not tx_info["ready"]:
            logging.warning(f"Cannot commit transaction that was not ready: {transaction_id}")
            return payment_pb2.CommitTransactionResponse(
                success=False,
                message=tx_info["message"]
            )
        
        details = tx_info["details"]
        account_id = details["account_id"]
        username = details["username"]
        transaction_type = details["type"]
        amount = details["amount"]
        counterparty = details["counterparty"]
        
        # Process the transaction
        try:
            # Update balance
            if transaction_type == "debit":
                self.users[username]["balance"] -= amount
            elif transaction_type == "credit":
                self.users[username]["balance"] += amount
            
            # Save changes
            self._save_user_credentials()
            
            # Record in transaction history
            self.record_transaction(
                account_id, 
                transaction_type, 
                amount, 
                counterparty, 
                transaction_id=transaction_id
            )
            
            # Remove from prepared transactions
            del self.prepared_transactions[transaction_id]
            
            logging.info(f"Transaction {transaction_id} committed successfully")
            return payment_pb2.CommitTransactionResponse(
                success=True,
                message=f"Transaction committed successfully"
            )
            
        except Exception as e:
            logging.error(f"Error committing transaction {transaction_id}: {str(e)}")
            return payment_pb2.CommitTransactionResponse(
                success=False,
                message=f"Error committing transaction: {str(e)}"
            )

    def AbortTransaction(self, request, context):
        """Phase 2 of 2PC: Abort the prepared transaction"""
        transaction_id = request.transaction_id
        
        logging.info(f"Abort transaction request: id={transaction_id}")
        
        # Check if this transaction was prepared
        if transaction_id not in self.prepared_transactions:
            logging.info(f"Transaction not found to abort: {transaction_id}")
            return payment_pb2.AbortTransactionResponse(
                success=True,
                message="Transaction not found, considered aborted"
            )
        
        # Remove from prepared transactions
        del self.prepared_transactions[transaction_id]
        
        logging.info(f"Transaction {transaction_id} aborted successfully")
        return payment_pb2.AbortTransactionResponse(
            success=True,
            message="Transaction aborted successfully"
        )            


                                                

def serve(bank_name, port):
    """Start the bank server with SSL/TLS enabled"""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s - {bank_name} - %(levelname)s - %(message)s'
    )
    
    # Load server key and certificate
    with open('certificate/server.key', 'rb') as f:
        server_key = f.read()
    with open('certificate/server.cert', 'rb') as f:
        server_cert = f.read()
    
    # Load CA certificate for client validation (for mutual TLS)
    with open('certificate/ca.cert', 'rb') as f:
        ca_cert = f.read()
    
    # Create SSL credentials
    server_credentials = grpc.ssl_server_credentials(
        [(server_key, server_cert)],
        root_certificates=ca_cert,
        require_client_auth=True  # Enable mutual TLS
    )
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add our service to the server
    payment_pb2_grpc.add_BankServiceServicer_to_server(
        BankServicer(bank_name), server
    )
    
    # Configure secure port
    server_address = f'[::]:{port}'
    server.add_secure_port(server_address, server_credentials)
    
    # Start the server
    server.start()
    logging.info(f"Bank Server {bank_name} started securely at {server_address}")
    
    # Keep server running until keyboard interrupt
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info(f"Bank Server {bank_name} shutting down...")
        server.stop(0)

if __name__ == '__main__':
    # Check command line arguments
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <bank_name> <port>")
        print("Example: python bank_server.py Bank1 50052")
        sys.exit(1)
    
    bank_name = sys.argv[1]
    port = int(sys.argv[2])
    
    serve(bank_name, port)
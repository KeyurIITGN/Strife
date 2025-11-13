import grpc
from concurrent import futures
import json
import uuid
import time
import logging
import os
import threading

import protofiles.payment_pb2 as payment_pb2
import protofiles.payment_pb2_grpc as payment_pb2_grpc

BANK_SERVERS = {
    "Bank1": "localhost:50052",
    "Bank2": "localhost:50053"
}

active_tokens = {}
PROCESSED_KEYS = {}

TPC_TIMEOUT_SECONDS = 10

TOKEN_STORAGE_FILE = "data/active_tokens.json"

def load_tokens():
    if not os.path.exists(TOKEN_STORAGE_FILE):
        return {}
    try:
        with open(TOKEN_STORAGE_FILE, 'r') as f:
            tokens = json.load(f)
            for token, info in tokens.items():
                if "expires" in info:
                    info["expires"] = float(info["expires"])

            logging.info(f"Loaded {len(tokens)} active tokens from storage")
            return tokens
    except Exception as e:
        logging.error(f"Error loading tokens from storage: {str(e)}")
        return {}

def save_tokens(tokens):
    """Save active tokens to persistent storage"""
    os.makedirs(os.path.dirname(TOKEN_STORAGE_FILE), exist_ok=True)
    
    try:
        with open(TOKEN_STORAGE_FILE, 'w') as f:
            json.dump(tokens, f, indent=2)
        logging.info(f"Saved {len(tokens)} active tokens to storage")
    except Exception as e:
        logging.error(f"Error saving tokens to storage: {str(e)}")   

def cleanup_expired_tokens():
    """Remove expired tokens and save the active ones"""
    global active_tokens
    current_time = time.time()
    expired_count = len(active_tokens)
    
    active_tokens = {
        token: info for token, info in active_tokens.items()
        if info["expires"] > current_time
    }
    
    expired_count -= len(active_tokens)
    
    if expired_count > 0:
        logging.info(f"Cleaned up {expired_count} expired tokens")
        save_tokens(active_tokens)
    
    # Schedule the next cleanup
    threading.Timer(3600, cleanup_expired_tokens).start()                 


class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        self.auth_required_methods = {
            '/payment.PaymentGateway/CheckBalance',
            '/payment.PaymentGateway/ProcessPayment',
            '/payment.PaymentGateway/GetTransactionHistory'
        }
        super().__init__()

    def intercept_service(self, continuation, handler_call_details):
        # Get method name
        method = handler_call_details.method
        
        # Skip authentication for the Authenticate method
        if method == '/payment.PaymentGateway/Authenticate':
            return continuation(handler_call_details)
        
        # Check if method requires authentication
        if method in self.auth_required_methods:
            # Extract metadata
            metadata = dict(handler_call_details.invocation_metadata)
            
            # Check if token is present
            if 'token' not in metadata:
                return self._unauthenticated_rpc()
            
            token = metadata['token']
            
            # Validate token exists and hasn't expired
            if token not in active_tokens or active_tokens[token]["expires"] < time.time():
                return self._unauthenticated_rpc()
            
            # Authorization checks based on method
            user_info = active_tokens[token]
            
            if method == '/payment.PaymentGateway/CheckBalance':
                # Extract account ID from request (will be done in the service method)
                # For authorization, we'll check this in the service method
                pass
                
            elif method == '/payment.PaymentGateway/ProcessPayment':
                # For initiating payments, we'll check balance sufficiency in the service method
                pass
                
            elif method == '/payment.PaymentGateway/GetTransactionHistory':
                # For transaction history, we'll check account ownership in the service method
                pass
        
        # Continue with the call if authentication passed
        return continuation(handler_call_details)

    #if token is inactive or expired
    def _unauthenticated_rpc(self):
        """Handler for unauthenticated requests"""
        def unauthenticated_rpc(request, context):
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid or missing authentication token')
            return None
        
        return grpc.unary_unary_rpc_method_handler(unauthenticated_rpc)   

class LoggingInterceptor(grpc.ServerInterceptor):
    """Interceptor for logging all gRPC requests and responses"""
    
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('gateway_logger')
    
    def intercept_service(self, continuation, handler_call_details):
        # Extract method name
        method = handler_call_details.method
        method_name = method.split('/')[-1]
        
        # Extract client info - metadata is a tuple of (key, value) pairs
        peer = 'unknown'
        metadata_dict = dict(handler_call_details.invocation_metadata)
        if 'peer' in metadata_dict:
            peer = metadata_dict['peer']
        
        # Log the request start
        self.logger.info(f"Request received: Method={method_name}, Client={peer}")
        
        # Get the original handler by calling continuation
        handler = continuation(handler_call_details)
        
        # If the handler doesn't exist, just return None
        if handler is None:
            return None
        
        # Extract token if present (for client identification)
        client_id = "unauthenticated"
        if 'token' in metadata_dict and metadata_dict['token'] in active_tokens:
            client_id = active_tokens[metadata_dict['token']].get('username', 'unknown')
        
        # Create a wrapper function that will be called with the request
        def new_handler(request, server_context):
            # Log request details
            request_dict = {}
            for field in request.DESCRIPTOR.fields:
                # Don't log passwords
                if field.name == 'password':
                    value = '********'
                else:
                    value = getattr(request, field.name)
                request_dict[field.name] = value
            
            self.logger.info(f"Request details: Method={method_name}, Client={client_id}, Data={request_dict}")
            
            try:
                # Call the actual handler method using handler.unary_unary
                if handler.request_streaming and handler.response_streaming:
                    response_iterator = handler.stream_stream(request, server_context)
                    # Can't log streaming responses easily
                    return response_iterator
                elif handler.request_streaming:
                    response = handler.stream_unary(request, server_context)
                elif handler.response_streaming:
                    response_iterator = handler.unary_stream(request, server_context)
                    # Can't log streaming responses easily
                    return response_iterator
                else:
                    # Most common case - unary-unary
                    response = handler.unary_unary(request, server_context)
                
                # Log response details for unary responses
                if not handler.response_streaming:
                    response_dict = {}
                    for field in response.DESCRIPTOR.fields:
                        value = getattr(response, field.name)
                        response_dict[field.name] = value
                    
                    # For transaction methods, log special information
                    if method_name == 'ProcessPayment':
                        transaction_amount = getattr(request, 'amount', 'N/A')
                        transaction_status = getattr(response, 'status', 'N/A')
                        self.logger.info(f"Transaction completed: Method={method_name}, Client={client_id}, "
                                        f"Amount={transaction_amount}, Status={transaction_status}")
                    
                    self.logger.info(f"Response sent: Method={method_name}, Client={client_id}, Status=OK, Data={response_dict}")
                
                return response
            
            except Exception as e:
                # Log exceptions
                self.logger.error(f"Error during {method_name}: Client={client_id}, Error={str(e)}")
                raise
        
        # Create a new handler with our wrapper function
        if handler.request_streaming and handler.response_streaming:
            return grpc.stream_stream_rpc_method_handler(
                new_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )
        elif handler.request_streaming:
            return grpc.stream_unary_rpc_method_handler(
                new_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )
        elif handler.response_streaming:
            return grpc.unary_stream_rpc_method_handler(
                new_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )
        else:
            return grpc.unary_unary_rpc_method_handler(
                new_handler,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer
            )       

class PaymentGatewayServicer(payment_pb2_grpc.PaymentGatewayServicer):

    def __init__(self):
        # Create connections to all bank servers
        self.bank_stubs = {}
        for bank_name, bank_address in BANK_SERVERS.items():
            self.bank_stubs[bank_name] = self._create_bank_stub(bank_address)
        
        logging.info(f"Payment Gateway initialized with connections to {len(self.bank_stubs)} banks")

    def _create_bank_stub(self, bank_address):
        """Create a secure connection to a bank server using mutual TLS"""
        try:
            # Load client key and certificate
            with open('certificate/server.key', 'rb') as f:
                client_key = f.read()
            with open('certificate/server.cert', 'rb') as f:
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
            channel = grpc.secure_channel(bank_address, credentials)
            
            # Create a stub
            return payment_pb2_grpc.BankServiceStub(channel)
        
        except Exception as e:
            logging.error(f"Failed to connect to bank at {bank_address}: {str(e)}")
            return None
        
    def Authenticate(self, request, context):
        """Authenticates a user and provides a session token"""
        username = request.username
        password = request.password
        bank_name = request.bank_name
        
        # Log authentication attempt (omitting password for security)
        logging.info(f"Authentication attempt: username={username}, bank={bank_name}")
        
        # Validate bank exists
        if bank_name not in self.bank_stubs or self.bank_stubs[bank_name] is None:
            error_msg = f"Bank {bank_name} not found or connection failed"
            logging.warning(f"Authentication failed: {error_msg}")
            
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return payment_pb2.AuthResponse(
                success=False,
                token="",
                message=error_msg
            )
        
        try:
            # Forward credentials to bank for verification
            verification_request = payment_pb2.CredentialVerificationRequest(
                username=username,
                password=password
            )
            verification_response = self.bank_stubs[bank_name].VerifyCredentials(verification_request)
            
            if verification_response.valid:
                # Generate a session token (valid for 1 hour)
                token = f"{username}-{uuid.uuid4()}"
                expiry = time.time() + 3600  # 1 hour from now
                
                # Store the token with user info
                active_tokens[token] = {
                    "username": username,
                    "bank": bank_name,
                    "account": verification_response.account_id,
                    "expires": expiry
                }

                save_tokens(active_tokens)
                
                # Log successful authentication
                logging.info(f"Authentication successful: username={username}, bank={bank_name}")
                
                return payment_pb2.AuthResponse(
                    success=True,
                    token=token,
                    message="Authentication successful"
                )
            else:
                # Log failed authentication
                logging.warning(f"Authentication failed: username={username}, bank={bank_name}, reason={verification_response.message}")
                
                context.set_code(grpc.StatusCode.UNAUTHENTICATED)
                context.set_details(verification_response.message)
                return payment_pb2.AuthResponse(
                    success=False,
                    token="",
                    message=verification_response.message
                )
        
        except grpc.RpcError as e:
            # Log RPC error
            logging.error(f"Bank communication error during authentication: {e.code().name} - {e.details()}")
            
            context.set_code(e.code())
            context.set_details(f"Bank communication error: {e.details()}")
            return payment_pb2.AuthResponse(
                success=False,
                token="",
                message=f"Authentication failed: {e.details()}"
            )
    def CheckBalance(self, request, context):
        """Returns the account balance for an authenticated user"""
        # Extract token from metadata
        metadata = dict(context.invocation_metadata())
        token = metadata.get('token')
        
        # Get user info from token
        user_info = active_tokens[token]
        bank_name = user_info["bank"]
        account_id = user_info["account"]
        
        # Authorization check: ensure the requested account belongs to the authenticated user
        if request.account_id and request.account_id != account_id:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(f"You are not authorized to view balance for account {request.account_id}")
            return payment_pb2.BalanceResponse(
                success=False,
                balance=0,
                message=f"Authorization failed: Not your account"
            )
        
        try:
            # Forward request to the appropriate bank
            bank_request = payment_pb2.BankBalanceRequest(account_id=account_id)
            bank_response = self.bank_stubs[bank_name].GetBalance(bank_request)
            
            logging.info(f"Balance check for user {user_info['username']} at bank {bank_name}")
            
            return payment_pb2.BalanceResponse(
                success=bank_response.success,
                balance=bank_response.balance,
                message=bank_response.message
            )
        
        except grpc.RpcError as e:
            logging.error(f"Error checking balance: {e.code().name}")
            
            context.set_code(e.code())
            context.set_details(f"Bank communication error: {e.details()}")
            return payment_pb2.BalanceResponse(
                success=False,
                balance=0,
                message=f"Failed to get balance: {e.details()}"
            )  


    def GetTransactionHistory(self, request, context):
        """Get transaction history with authorization check"""
        # Extract token from metadata
        metadata = dict(context.invocation_metadata())
        token = metadata.get('token')
        
        # Get user info from token
        user_info = active_tokens[token]
        bank_name = user_info["bank"]
        account_id = user_info["account"]
        
        # Authorization check: ensure the requested account belongs to the authenticated user
        if request.account_id != account_id:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(f"You are not authorized to view transactions for account {request.account_id}")
            return payment_pb2.HistoryResponse(
                success=False,
                message="Authorization failed: Not your account"
            )
        
        try:
            # Forward request to the appropriate bank
            limit = request.limit if request.limit > 0 else 10  # Default to 10 transactions if not specified
            
            bank_request = payment_pb2.BankHistoryRequest(
                account_id=account_id,
                limit=limit
            )
            
            # Call the bank's GetTransactionHistory method
            bank_response = self.bank_stubs[bank_name].GetTransactionHistory(bank_request)
            
            logging.info(f"Transaction history retrieved for user {user_info['username']} at bank {bank_name}")
            
            # Return the history response to the client
            return payment_pb2.HistoryResponse(
                success=bank_response.success,
                transactions=bank_response.transactions,
                message=bank_response.message
            )
            
        except grpc.RpcError as e:
            logging.error(f"Error retrieving transaction history: {e.code().name}")
            
            context.set_code(e.code())
            context.set_details(f"Bank communication error: {e.details()}")
            return payment_pb2.HistoryResponse(
                success=False,
                message=f"Failed to retrieve transaction history: {e.details()}"
            )
        
    def ProcessPayment(self, request, context):
        """Process a payment with idempotency guarantee"""
        # Extract token directly from request
        token = request.token
        
        # Verify token is valid
        if token not in active_tokens:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details("Invalid authentication token")
            return payment_pb2.PaymentResponse(
                success=False,
                status="failed",
                message="Authentication failed: Invalid token"
            )
        
        # Get sender info from token
        sender_info = active_tokens[token]
        sender_bank = sender_info["bank"]
        sender_account = sender_info["account"]
        
        # Authorization check: ensure the sender account belongs to the authenticated user
        if request.sender_account != "self" and request.sender_account != sender_account:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(f"You are not authorized to initiate payments from account {request.sender_account}")
            return payment_pb2.PaymentResponse(
                success=False,
                status="failed",
                message="Authorization failed: Not your account"
            )
        
        # Idempotency check: If we've seen this payment_id before, return the cached result
        payment_id = request.payment_id
        if payment_id and payment_id in PROCESSED_KEYS:
            cached_response = PROCESSED_KEYS[payment_id]
            if cached_response.success or not self._is_retriable_error(cached_response):
                logging.info(f"Returning cached result for idempotent request: {payment_id}")
                return cached_response
            else:
                logging.info(f"Previous attempt failed. So trying again: {payment_id}")

        
        # Get receiver info from request
        receiver_bank = request.receiver_bank
        receiver_account = request.receiver_account
        amount = request.amount
        
        # Check if receiver bank exists
        if receiver_bank not in self.bank_stubs:
            error_msg = f"Receiver bank {receiver_bank} not found"
            response = payment_pb2.PaymentResponse(
                success=False,
                status="failed",
                transaction_id="",
                message=error_msg
            )
            
            # Cache the response for idempotency
            if payment_id:
                PROCESSED_KEYS[payment_id] = response
            
            return response
        
        # Check if sender has sufficient funds
        try:
            balance_request = payment_pb2.BankBalanceRequest(account_id=sender_account)
            balance_response = self.bank_stubs[sender_bank].GetBalance(balance_request)
            
            if not balance_response.success:
                response = payment_pb2.PaymentResponse(
                    success=False,
                    status="failed",
                    transaction_id="",
                    message=f"Could not verify balance: {balance_response.message}"
                )
                
                # Cache the response for idempotency
                if payment_id:
                    PROCESSED_KEYS[payment_id] = response
                
                return response
            
            # Authorization check: ensure the sender has sufficient funds
            if balance_response.balance < amount:
                context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                context.set_details("Insufficient funds")
                response = payment_pb2.PaymentResponse(
                    success=False,
                    status="failed",
                    transaction_id="",
                    message=f"Authorization failed: Insufficient funds. Available: {balance_response.balance}, Required: {amount}"
                )
                
                # Cache the response for idempotency
                if payment_id:
                    PROCESSED_KEYS[payment_id] = response
                
                return response
            
            # Perform the payment using Two-Phase Commit (implemented in a separate function)
            payment_response = self._process_payment_2pc(
                payment_id,
                sender_bank, sender_account,
                receiver_bank, receiver_account,
                amount
            )
            
            # Cache the response for idempotency
            if payment_id:
                if payment_response.success or not self._is_retriable_error(payment_response):
                    PROCESSED_KEYS[payment_id] = payment_response
            
            return payment_response
        
        except grpc.RpcError as e:
            logging.error(f"Error during payment: {e.code().name}")
            context.set_code(e.code())
            context.set_details(f"Bank communication error: {e.details()}")
            response = payment_pb2.PaymentResponse(
                success=False,
                status="failed",
                transaction_id="",
                message=f"Payment failed: {e.details()}"
            )
            
            # Cache the response for idempotency
            if payment_id:
                PROCESSED_KEYS[payment_id] = response
            
            return response
    
    def _is_retriable_error(self, response):
        retriable_error = [
            "Bank communication error",
            "Connection Failed",
            "Timeout",
            "Server unavailable"
        ]
        
    def _process_payment_2pc(self, payment_id, sender_bank, sender_account, receiver_bank, receiver_account, amount):
        """Process payment using proper Two-Phase Commit with timeout"""
        global_transaction_id = str(uuid.uuid4())
        
        # Log payment request
        logging.info(f"Processing payment with 2PC: {amount} from {sender_bank}/{sender_account} to {receiver_bank}/{receiver_account}")
        
        # Skip the actual transfer if sender and receiver are the same
        if sender_bank == receiver_bank and sender_account == receiver_account:
            logging.info(f"Self-transfer detected, no actual transfer needed")
            return payment_pb2.PaymentResponse(
                success=True,
                transaction_id=global_transaction_id,
                status="completed",
                message="Self-transfer processed successfully (no balance change)"
            )
        
        try:
            # Check if banks exist
            if sender_bank not in self.bank_stubs:
                return payment_pb2.PaymentResponse(
                    success=False,
                    transaction_id="",
                    status="failed",
                    message=f"Sender bank {sender_bank} not found"
                )
                
            if receiver_bank not in self.bank_stubs:
                return payment_pb2.PaymentResponse(
                    success=False,
                    transaction_id="",
                    status="failed",
                    message=f"Receiver bank {receiver_bank} not found"
                )
            
            # Generate unique transaction IDs for each part
            sender_tx_id = f"{global_transaction_id}-sender-{payment_id}"
            receiver_tx_id = f"{global_transaction_id}-receiver-{payment_id}"
            
            # Record start time for timeout tracking
            start_time = time.time()
            
            # PHASE 1: Prepare - Ask all participants to vote
            logging.info(f"2PC Phase 1: Prepare transactions")
            
            # Create prepare requests
            sender_prepare_request = payment_pb2.PrepareTransactionRequest(
                transaction_id=sender_tx_id,
                account_id=sender_account,
                type="debit",
                amount=amount,
                counterparty=f"{receiver_bank}/{receiver_account}"
            )
            
            receiver_prepare_request = payment_pb2.PrepareTransactionRequest(
                transaction_id=receiver_tx_id,
                account_id=receiver_account,
                type="credit",
                amount=amount,
                counterparty=f"{sender_bank}/{sender_account}"
            )
            
            # Set timeout for prepare phase
            deadline = time.time() + TPC_TIMEOUT_SECONDS
            
            # Prepare sender transaction (debit) with timeout
            try:
                sender_prepare_response = self.bank_stubs[sender_bank].PrepareTransaction(
                    sender_prepare_request,
                    timeout=max(0, deadline - time.time())  # Remaining time until deadline
                )
                
                if not sender_prepare_response.ready:
                    # If sender cannot prepare, abort the transaction
                    logging.warning(f"Sender bank voted NO: {sender_prepare_response.message}")
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message=f"Sender bank cannot process: {sender_prepare_response.message}"
                    )
                    
            except grpc.RpcError as e:
                # Handle timeout or other RPC errors
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logging.error(f"Timeout while preparing transaction with sender bank")
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message="Transaction timed out during preparation (sender)"
                    )
                else:
                    logging.error(f"Error preparing transaction with sender bank: {e.code().name}")
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message=f"Error communicating with sender bank: {e.code().name}"
                    )
            
            # Check if we're approaching timeout
            if time.time() > deadline - 1:  # Leave 1 second buffer
                logging.error(f"Approaching timeout, aborting transaction")
                # Abort the prepared transaction at sender
                try:
                    abort_sender_request = payment_pb2.AbortTransactionRequest(
                        transaction_id=sender_tx_id
                    )
                    self.bank_stubs[sender_bank].AbortTransaction(
                        abort_sender_request, 
                        timeout=2  # Short timeout for abort
                    )
                except Exception as e:
                    logging.error(f"Error aborting sender transaction after timeout: {str(e)}")
                    
                return payment_pb2.PaymentResponse(
                    success=False,
                    transaction_id=global_transaction_id,
                    status="failed",
                    message="Transaction timed out during preparation phase"
                )
            
            # Prepare receiver transaction (credit) with timeout
            try:
                receiver_prepare_response = self.bank_stubs[receiver_bank].PrepareTransaction(
                    receiver_prepare_request,
                    timeout=max(0, deadline - time.time())  # Remaining time until deadline
                )
                
                if not receiver_prepare_response.ready:
                    # If receiver cannot prepare, abort both transactions
                    logging.warning(f"Receiver bank voted NO: {receiver_prepare_response.message}")
                    
                    # Abort sender transaction
                    try:
                        abort_sender_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=sender_tx_id
                        )
                        self.bank_stubs[sender_bank].AbortTransaction(
                            abort_sender_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as e:
                        logging.error(f"Error aborting sender transaction: {str(e)}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message=f"Receiver bank cannot process: {receiver_prepare_response.message}"
                    )
                    
            except grpc.RpcError as e:
                # Handle timeout or other RPC errors
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logging.error(f"Timeout while preparing transaction with receiver bank")
                    
                    # Abort sender transaction
                    try:
                        abort_sender_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=sender_tx_id
                        )
                        self.bank_stubs[sender_bank].AbortTransaction(
                            abort_sender_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as abort_e:
                        logging.error(f"Error aborting sender transaction: {str(abort_e)}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message="Transaction timed out during preparation (receiver)"
                    )
                else:
                    logging.error(f"Error preparing transaction with receiver bank: {e.code().name}")
                    
                    # Abort sender transaction
                    try:
                        abort_sender_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=sender_tx_id
                        )
                        self.bank_stubs[sender_bank].AbortTransaction(
                            abort_sender_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as abort_e:
                        logging.error(f"Error aborting sender transaction: {str(abort_e)}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message=f"Error communicating with receiver bank: {e.code().name}"
                    )
            
            # Check if we've exceeded the timeout
            if time.time() > deadline - 1:  # Leave 1 second buffer
                logging.error(f"Approaching timeout after preparation phase, aborting transaction")
                
                # Abort both prepared transactions
                try:
                    abort_sender_request = payment_pb2.AbortTransactionRequest(
                        transaction_id=sender_tx_id
                    )
                    self.bank_stubs[sender_bank].AbortTransaction(
                        abort_sender_request, 
                        timeout=2  # Short timeout for abort
                    )
                except Exception as e:
                    logging.error(f"Error aborting sender transaction after timeout: {str(e)}")
                    
                try:
                    abort_receiver_request = payment_pb2.AbortTransactionRequest(
                        transaction_id=receiver_tx_id
                    )
                    self.bank_stubs[receiver_bank].AbortTransaction(
                        abort_receiver_request, 
                        timeout=2  # Short timeout for abort
                    )
                except Exception as e:
                    logging.error(f"Error aborting receiver transaction after timeout: {str(e)}")
                    
                return payment_pb2.PaymentResponse(
                    success=False,
                    transaction_id=global_transaction_id,
                    status="failed",
                    message="Transaction timed out before commit phase"
                )
            
            # PHASE 2: Commit - Both banks voted YES, so commit the transactions
            logging.info(f"2PC Phase 2: Commit transactions")
            
            # Set a new deadline for commit phase
            deadline = time.time() + TPC_TIMEOUT_SECONDS
            
            # Commit sender transaction (debit) with timeout
            try:
                commit_sender_request = payment_pb2.CommitTransactionRequest(
                    transaction_id=sender_tx_id
                )
                
                commit_sender_response = self.bank_stubs[sender_bank].CommitTransaction(
                    commit_sender_request,
                    timeout=max(0, deadline - time.time())
                )
                
                if not commit_sender_response.success:
                    # If sender commit fails, this is a critical error in 2PC
                    logging.error(f"Critical 2PC error: Sender commit failed after both voted YES: {commit_sender_response.message}")
                    
                    # Abort receiver transaction
                    try:
                        abort_receiver_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=receiver_tx_id
                        )
                        self.bank_stubs[receiver_bank].AbortTransaction(
                            abort_receiver_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as e:
                        logging.error(f"Error aborting receiver transaction: {str(e)}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message=f"Transaction failed during commit phase: {commit_sender_response.message}"
                    )
                    
            except grpc.RpcError as e:
                # Handle timeout or other RPC errors
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logging.error(f"Timeout while committing transaction with sender bank")
                    
                    # This is a critical state - we need to try to abort the receiver transaction
                    try:
                        abort_receiver_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=receiver_tx_id
                        )
                        self.bank_stubs[receiver_bank].AbortTransaction(
                            abort_receiver_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as abort_e:
                        logging.error(f"Error aborting receiver transaction: {str(abort_e)}")
                    
                    # We also need to try to abort the sender transaction, but it may have already committed
                    try:
                        abort_sender_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=sender_tx_id
                        )
                        self.bank_stubs[sender_bank].AbortTransaction(
                            abort_sender_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as abort_e:
                        logging.error(f"Error aborting sender transaction: {str(abort_e)}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message="Transaction timed out during commit phase (sender)"
                    )
                else:
                    logging.error(f"Error committing transaction with sender bank: {e.code().name}")
                    
                    # Abort receiver transaction
                    try:
                        abort_receiver_request = payment_pb2.AbortTransactionRequest(
                            transaction_id=receiver_tx_id
                        )
                        self.bank_stubs[receiver_bank].AbortTransaction(
                            abort_receiver_request, 
                            timeout=2  # Short timeout for abort
                        )
                    except Exception as abort_e:
                        logging.error(f"Error aborting receiver transaction: {str(abort_e)}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="failed",
                        message=f"Error committing to sender bank: {e.code().name}"
                    )
            
            # Check if we've exceeded the timeout
            if time.time() > deadline - 1:  # Leave 1 second buffer
                logging.error(f"Approaching timeout after sender commit, critical state")
                
                # This is a critical state - sender committed but receiver hasn't
                # We should still try to commit the receiver to maintain consistency
                # In a real system, this would require a recovery protocol
                
                return payment_pb2.PaymentResponse(
                    success=False,
                    transaction_id=global_transaction_id,
                    status="error",
                    message="CRITICAL ERROR: Transaction timed out after sender committed. System may be in inconsistent state."
                )
            
            # Commit receiver transaction (credit) with timeout
            try:
                commit_receiver_request = payment_pb2.CommitTransactionRequest(
                    transaction_id=receiver_tx_id
                )
                
                commit_receiver_response = self.bank_stubs[receiver_bank].CommitTransaction(
                    commit_receiver_request,
                    timeout=max(0, deadline - time.time())
                )
                
                if not commit_receiver_response.success:
                    # If receiver commit fails, this is a critical error in 2PC
                    logging.error(f"Critical 2PC error: Receiver commit failed after sender committed: {commit_receiver_response.message}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="error",
                        message=f"CRITICAL ERROR: Sender debited but receiver credit failed: {commit_receiver_response.message}"
                    )
                    
            except grpc.RpcError as e:
                # Handle timeout or other RPC errors
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logging.error(f"Timeout while committing transaction with receiver bank")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="error",
                        message="CRITICAL ERROR: Transaction timed out during commit phase (receiver). Sender was debited but receiver may not be credited."
                    )
                else:
                    logging.error(f"Error committing transaction with receiver bank: {e.code().name}")
                    
                    return payment_pb2.PaymentResponse(
                        success=False,
                        transaction_id=global_transaction_id,
                        status="error",
                        message=f"CRITICAL ERROR: Error committing to receiver bank: {e.code().name}. Sender was debited but receiver may not be credited."
                    )
            
            # Calculate and log total transaction time
            total_time = time.time() - start_time
            logging.info(f"2PC completed successfully in {total_time:.2f} seconds for transaction {global_transaction_id}")
            
            # Both transactions committed successfully
            return payment_pb2.PaymentResponse(
                success=True,
                transaction_id=global_transaction_id,
                status="completed",
                message="Payment processed successfully"
            )
            
        except Exception as e:
            logging.error(f"Unexpected error during 2PC: {str(e)}")
            
            # Try to abort any prepared transactions
            try:
                if 'sender_tx_id' in locals():
                    abort_sender_request = payment_pb2.AbortTransactionRequest(
                        transaction_id=sender_tx_id
                    )
                    self.bank_stubs[sender_bank].AbortTransaction(
                        abort_sender_request,
                        timeout=2  # Short timeout for abort
                    )
                
                if 'receiver_tx_id' in locals():
                    abort_receiver_request = payment_pb2.AbortTransactionRequest(
                        transaction_id=receiver_tx_id
                    )
                    self.bank_stubs[receiver_bank].AbortTransaction(
                        abort_receiver_request,
                        timeout=2  # Short timeout for abort
                    )
            except Exception as abort_error:
                logging.error(f"Error during abort after exception: {str(abort_error)}")
            
            return payment_pb2.PaymentResponse(
                success=False,
                transaction_id=global_transaction_id if 'global_transaction_id' in locals() else "",
                status="failed",
                message=f"Payment failed due to unexpected error: {str(e)}"
            )

def serve():


    os.makedirs("logs", exist_ok=True)


        # Clear previous log file
    with open("logs/gateway.log", 'w') as f:
        f.write("")  # Write empty string to clear the file
    
    # Configure both the root logger and the gateway_logger
    logger_format = '%(asctime)s - PaymentGateway - %(levelname)s - %(message)s'
    
    # Root logger setup
    logging.basicConfig(
        level=logging.INFO,
        format=logger_format,
        handlers=[
            logging.FileHandler("logs/gateway.log"),
            logging.StreamHandler()
        ]
    )
    
    # Configure gateway_logger separately
    gateway_logger = logging.getLogger('gateway_logger')
    gateway_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers to avoid duplicates
    if gateway_logger.handlers:
        gateway_logger.handlers.clear()
        
    # Add the same handlers to gateway_logger
    file_handler = logging.FileHandler("logs/gateway.log")
    file_handler.setFormatter(logging.Formatter(logger_format))
    gateway_logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(logger_format))
    gateway_logger.addHandler(console_handler)
    
    # Ensure gateway_logger doesn't propagate to root logger to avoid duplicate logs
    gateway_logger.propagate = False




    global active_tokens
    active_tokens = load_tokens()
    current_time = time.time()
    active_tokens = {
        token: info for token, info in active_tokens.items()
        if info["expires"] > current_time
    }

    #Why am I setting up logging here? 
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - PaymentGateway - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/gateway.log"),
            logging.StreamHandler()
        ]
    )

    cleanup_expired_tokens()

    with open('certificate/server.key', 'rb') as f:
        server_key = f.read()
    with open('certificate/server.cert', 'rb') as f:
        server_cert = f.read()

    
    with open('certificate/ca.cert', 'rb') as f:
        ca_cert = f.read()
    
        server_credentials = grpc.ssl_server_credentials(
        [(server_key, server_cert)],
        root_certificates=ca_cert,
        require_client_auth=True  # Enable mutual TLS
    )


    auth_interceptor = AuthInterceptor()
    logging_interceptor = LoggingInterceptor()    

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10),
                         interceptors=[auth_interceptor, logging_interceptor])

    payment_pb2_grpc.add_PaymentGatewayServicer_to_server(
        PaymentGatewayServicer(), server
    )

    server_address = '[::]:50051'
    server.add_secure_port(server_address, server_credentials)

    server.start()
    logging.info(f"Payment Gateway started securely at {server_address}")


    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info("Payment Gateway shutting down...")
        server.stop(0)

if __name__ == '__main__':
    serve()






import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def read_private_key(path):
    with open(path, 'rb') as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
    return private_key

def connect_to_snowflake():
    # Private key configuration
    
    # Reading an unencrypted key
    private_key = read_private_key('private.pem')

    # Convert the key to bytes
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Connect to Snowflake
    connection = snowflake.connector.connect(
        user='ECDP-MLOPS-SERVICE-USER',
        account='VKPVQAF-ECDP_NONPROD',
        private_key=pkb,
        database='ML_PIPELINES_DATA',      # Optional
        schema='PUBLIC',   # Optional
        role='ECDP-MLOPS-SERVICE-ROLE' # Optional
    )
    
    return connection

def main():
    try:
        conn = connect_to_snowflake()
        print('Successfully connected to Snowflake!')
        
        # Execute your queries here
        cursor = conn.cursor()
        # cursor.execute("YOUR QUERY")
        print('Connection created!')
        
    except Exception as e:
        print(f'Connection failed: {str(e)}')
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()

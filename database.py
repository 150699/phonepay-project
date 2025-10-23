import mysql.connector

# Create connection
def database_connection():
    # Database connection setup 
    try:
        db = mysql.connector.connect(
        host="localhost",      
        user="root",          
        password="Praveen@11",  
        database="phone_pay")        
        return db
    
    except  mysql.connector.Error as e:
        #print(f"Error connecting to the database: {e}")
        return f"Error: {str(e)}"
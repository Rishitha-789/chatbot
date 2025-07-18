from chatbot import process_query

def main():
    print("Welcome to the Short Position Chatbot!")
    while True:
        query = input("Ask a question (type 'exit' to quit): ")
        if query.lower() == 'exit':
            break
        response = process_query(query)
        print(response)

if __name__ == '__main__':
    main()

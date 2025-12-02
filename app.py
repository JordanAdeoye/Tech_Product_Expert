import streamlit as st


from rag_retrieval import query_data_rag


import chromadb
client = chromadb.PersistentClient("./chroma_data")

collection = client.get_or_create_collection(name="youtube_transcripts")
st.title("Tech Expert")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if prompt := st.chat_input("What is up?"):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # response = f"Echo: {prompt}"
    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        response = st.write_stream(query_data_rag(prompt,client))
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})



# query = "How is the battery life on the Samsung Z Fold 7?"
# query = "whats are the states in nigeria"
# return_response = query_data(query,chunk_vector.client)


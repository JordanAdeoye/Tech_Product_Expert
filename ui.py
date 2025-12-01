import streamlit as st
from query_data import query_data_rag
import chunk_vector

st.title("Tech Expert")

# st.markdown("""
# <style>
# .fixed-header {
#     position: fixed;
#     top: 0;
#     left: 0;
#     right: 0;
#     padding: 16px 3rem;
#     background-color: white;
#     z-index: 999;
#     border-bottom: 1px solid #eee;
#     font-size: 28px;
#     font-weight: 700;
# }

# /* This is the main page container in current Streamlit */
# .block-container {
#     padding-top: 90px;  /* space so chat isn’t hidden behind header */
# }
# </style>
# """, unsafe_allow_html=True)

# # 🔹 2. Actual header
# st.markdown('<div class="fixed-header">Tech Expert</div>', unsafe_allow_html=True)


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
        response = st.write_stream(query_data_rag(prompt,chunk_vector.client))
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})



# query = "How is the battery life on the Samsung Z Fold 7?"
# query = "whats are the states in nigeria"
# return_response = query_data(query,chunk_vector.client)
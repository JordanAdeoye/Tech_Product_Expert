import streamlit as st
from langchain_classic.memory import ConversationBufferMemory
import chromadb
import os
from rag_retrieval import query_data_rag

from dotenv import load_dotenv
load_dotenv()

API_KEY_CHROMA =  os.getenv("API_KEY_CHROMA")
CHROMA_TENANT_ID=os.getenv("CHROMA_TENANT_ID")
CHROMA_DATABASE=os.getenv("database")

client = chromadb.CloudClient(
  api_key=API_KEY_CHROMA,
  tenant=CHROMA_TENANT_ID,
  database=CHROMA_DATABASE
)


st.title("Tech Expert")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "memory" not in st.session_state:
    st.session_state.memory = ConversationBufferMemory(
        memory_key="history",
        return_messages=False,  # since you're using a string PromptTemplate
    )

memory = st.session_state.memory

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
        response = st.write_stream(query_data_rag(prompt,client,memory))
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})































# import streamlit as st
# from langchain_classic.memory import ConversationBufferMemory
# from rag_retrieval import query_data_rag
# import rag_indexing_pipeline

# # Page configuration
# st.set_page_config(
#     page_title="Tech Expert AI",
#     page_icon="🤖",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )

# # Custom CSS for better styling
# st.markdown("""
#     <style>
#     /* Main container styling */
#     .main {
#         padding: 2rem;
#     }
    
#     /* Header styling */
#     .main-header {
#         text-align: center;
#         padding: 2rem 0;
#         background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
#         border-radius: 15px;
#         margin-bottom: 2rem;
#         box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
#     }
    
#     .main-header h1 {
#         color: white;
#         font-size: 2.5rem;
#         margin: 0;
#         font-weight: 700;
#     }
    
#     .main-header p {
#         color: rgba(255, 255, 255, 0.9);
#         font-size: 1.1rem;
#         margin-top: 0.5rem;
#     }
    
#     /* Chat message styling */
#     .stChatMessage {
#         padding: 1rem;
#         border-radius: 10px;
#         margin-bottom: 1rem;
#     }
    
#     /* Input box styling */
#     .stChatInputContainer {
#         padding: 1rem 0;
#     }
    
#     /* Sidebar styling */
#     .css-1d391kg {
#         background-color: #f8f9fa;
#     }
    
#     /* Button styling */
#     .stButton button {
#         background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
#         color: white;
#         border: none;
#         border-radius: 8px;
#         padding: 0.5rem 2rem;
#         font-weight: 600;
#         transition: all 0.3s ease;
#     }
    
#     .stButton button:hover {
#         transform: translateY(-2px);
#         box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
#     }
    
#     /* Stats card styling */
#     .stats-card {
#         background: white;
#         padding: 1.5rem;
#         border-radius: 10px;
#         box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
#         margin-bottom: 1rem;
#     }
    
#     .stats-card h3 {
#         color: #667eea;
#         margin: 0 0 0.5rem 0;
#         font-size: 1.2rem;
#     }
    
#     .stats-card p {
#         color: #666;
#         margin: 0;
#         font-size: 2rem;
#         font-weight: 700;
#     }
#     </style>
# """, unsafe_allow_html=True)

# # Header
# st.markdown("""
#     <div class="main-header">
#         <h1>🤖 Tech Expert AI</h1>
#         <p>Your intelligent assistant for technical questions</p>
#     </div>
# """, unsafe_allow_html=True)

# # Sidebar
# with st.sidebar:
#     st.markdown("### ⚙️ Settings")
    
#     # Clear chat button
#     if st.button("🗑️ Clear Conversation", use_container_width=True):
#         st.session_state.messages = []
#         st.session_state.memory = ConversationBufferMemory(
#             memory_key="history",
#             return_messages=False,
#         )
#         st.rerun()
    
#     st.markdown("---")
    
#     # Stats
#     st.markdown("### 📊 Session Stats")
    
#     message_count = len(st.session_state.get("messages", []))
#     user_messages = len([m for m in st.session_state.get("messages", []) if m["role"] == "user"])
    
#     col1, col2 = st.columns(2)
#     with col1:
#         st.metric("Total Messages", message_count)
#     with col2:
#         st.metric("Your Questions", user_messages)
    
#     st.markdown("---")
    
#     # Information
#     st.markdown("### ℹ️ About")
#     st.markdown("""
#     This AI assistant uses RAG (Retrieval-Augmented Generation) to provide 
#     accurate, context-aware responses to your technical questions.
    
#     **Features:**
#     - 💬 Conversational memory
#     - 🔍 Context-aware responses
#     - 📚 Knowledge retrieval
#     - ⚡ Real-time streaming
#     """)
    
#     st.markdown("---")
#     st.markdown("**Tips for better responses:**")
#     st.markdown("""
#     - Be specific with your questions
#     - Provide context when needed
#     - Ask follow-up questions
#     - Reference previous topics
#     """)

# # Initialize chat history
# if "messages" not in st.session_state:
#     st.session_state.messages = []

# if "memory" not in st.session_state:
#     st.session_state.memory = ConversationBufferMemory(
#         memory_key="history",
#         return_messages=False,
#     )

# memory = st.session_state.memory

# # Main chat area
# chat_container = st.container()

# with chat_container:
#     # Display welcome message if no messages
#     if len(st.session_state.messages) == 0:
#         st.markdown("""
#             <div style="text-align: center; padding: 3rem 1rem; color: #666;">
#                 <h3>👋 Welcome! How can I help you today?</h3>
#                 <p>Ask me anything about technology, programming, or technical concepts.</p>
#             </div>
#         """, unsafe_allow_html=True)
    
#     # Display chat messages from history
#     for message in st.session_state.messages:
#         with st.chat_message(message["role"], avatar="👤" if message["role"] == "user" else "🤖"):
#             st.markdown(message["content"])

# # Chat input at the bottom
# if prompt := st.chat_input("Ask me anything about technology..."):
#     # Display user message
#     with st.chat_message("user", avatar="👤"):
#         st.markdown(prompt)
    
#     # Add user message to chat history
#     st.session_state.messages.append({"role": "user", "content": prompt})
    
#     # Display assistant response
#     with st.chat_message("assistant", avatar="🤖"):
#         with st.spinner("Thinking..."):
#             response = st.write_stream(query_data_rag(prompt, rag_indexing_pipeline.client, memory))
    
#     # Add assistant response to chat history
#     st.session_state.messages.append({"role": "assistant", "content": response})

# # query = "How is the battery life on the Samsung Z Fold 7?"
# # query = "whats are the states in nigeria"
# # return_response = query_data(query,chunk_vector.client)


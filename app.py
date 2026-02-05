import streamlit as st
from langchain_classic.memory import ConversationBufferMemory
from rag_retrieval import query_data_rag
<<<<<<< HEAD
# import rag_indexing_pipeline
=======
<<<<<<< Updated upstream
import rag_indexing_pipeline
=======
>>>>>>> Stashed changes
>>>>>>> 79d14a8 (fixes)
import chromadb
import os
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




# Page configuration
# Page configuration
# Page configuration
st.set_page_config(
    page_title="Tech Expert AI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    /* Main container styling */
    .main {
        padding: 2rem;
    }
    
    /* Header styling */
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(135deg, #1e3a8a 0%, #FFA500 100%);
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .main-header h1 {
        color: white;
        font-size: 2.5rem;
        margin: 0;
        font-weight: 700;
    }
    
    .main-header p {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1rem;
        margin-top: 0.5rem;
    }
    
    /* Chat message styling */
    .stChatMessage {
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }
    
    /* Input box styling */
    .stChatInputContainer {
        padding: 1rem 0;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: #f8f9fa;
    }
    
    /* Button styling */
    .stButton button {
        background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
    }
    
    /* Stats card styling */
    .stats-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    
    .stats-card h3 {
        color: #1e3a8a;
        margin: 0 0 0.5rem 0;
        font-size: 1.2rem;
    }
    
    .stats-card p {
        color: #666;
        margin: 0;
        font-size: 2rem;
        font-weight: 700;
    }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
    <div class="main-header">
        <h1>🤖 TechTalk</h1>
        <p>Your AI advisor for consumer electronics</p>
    </div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### ⚙️ Settings")
    
    # Clear chat button
    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.memory = ConversationBufferMemory(
            memory_key="history",
            return_messages=False,
        )
        st.rerun()
    
    st.markdown("---")
    
    # Information
    st.markdown("### About")
    st.markdown("""
    TechTalk is your AI-powered advisor for consumer electronics, providing answers based on real YouTube tech reviews.
    
    **How it works:**
    - Analyzes transcripts from unbiased tech reviewers (MKBHD, Mrwhosetheboss, JerryRigEverything, LinusTechTips, and more)
    - Provides answers based on actual expert reviews, not generic info
    
    **What you can ask:**
    - Product comparisons and recommendations
    - Specs, features, and performance details
    - Battery life, durability, and real-world usage
    - Latest releases and updates
    """)
    
    st.markdown("---")
    st.markdown("### 👨‍💻 Developer")
    st.markdown("""
    Built by Jordan Adeoye
    
    [View My Portfolio →](http://jordanadeoye.github.io/portfolio/)
    """)
    

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "memory" not in st.session_state:
    st.session_state.memory = ConversationBufferMemory(
        memory_key="history",
        return_messages=False,
    )

memory = st.session_state.memory

# Main chat area
chat_container = st.container()

with chat_container:
    # Display welcome message if no messages
    if len(st.session_state.messages) == 0:
        st.markdown("""
            <div style="text-align: center; padding: 3rem 1rem; color: #666;">
                <h3>Welcome! How can I help you today?</h3>
                <p>Ask me about phones, laptops, tablets, smartwatches, and other consumer electronics.</p>
            </div>
        """, unsafe_allow_html=True)
    
    # Display chat messages from history
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar="👤" if message["role"] == "user" else "🤖"):
            st.markdown(message["content"])

# Chat input at the bottom
if prompt := st.chat_input("Ask me about phones, laptops, and other electronics..."):
    # Display user message
    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)
    
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display assistant response
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("Thinking..."):
            response = st.write_stream(query_data_rag(prompt, client, memory))
    
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})




# query = "How is the battery life on the Samsung Z Fold 7?"
# query = "whats are the states in nigeria"
# return_response = query_data(query,chunk_vector.client)




# st.title("Tech Expert")

# # Initialize chat history
# if "messages" not in st.session_state:
#     st.session_state.messages = []

# if "memory" not in st.session_state:
#     st.session_state.memory = ConversationBufferMemory(
#         memory_key="history",
#         return_messages=False,  # since you're using a string PromptTemplate
#     )

# memory = st.session_state.memory

# # Display chat messages from history on app rerun
# for message in st.session_state.messages:
#     with st.chat_message(message["role"]):
#         st.markdown(message["content"])

# # React to user input
# if prompt := st.chat_input("What is up?"):
#     # Display user message in chat message container
#     st.chat_message("user").markdown(prompt)
#     # Add user message to chat history
#     st.session_state.messages.append({"role": "user", "content": prompt})

#     # response = f"Echo: {prompt}"
#     # Display assistant response in chat message container
#     with st.chat_message("assistant"):
#         response = st.write_stream(query_data_rag(prompt,client,memory))
#     # Add assistant response to chat history
#     st.session_state.messages.append({"role": "assistant", "content": response})



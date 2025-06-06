import streamlit as st
import polars as pl
from google.cloud import storage
from datetime import datetime
import io
import json

# Initialize GCP Storage client
@st.cache_resource
def init_gcp_client():
    """Initialize and cache the GCP storage client"""
    # You'll need to set up authentication - either through service account key or environment
    return storage.Client()

def upload_to_gcp(file_obj, institution, filename, bucket_name="your-finance-bucket"):
    """Upload file to GCP bucket"""
    try:
        client = init_gcp_client()
        bucket = client.bucket(bucket_name)
        
        # Create path: institution/timestamp_filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_path = f"{institution}/{timestamp}_{filename}"
        
        blob = bucket.blob(blob_path)
        file_obj.seek(0)  # Reset file pointer
        blob.upload_from_file(file_obj)
        
        return blob_path
    except Exception as e:
        st.error(f"Upload failed: {str(e)}")
        return None

def list_files_from_gcp(bucket_name="your-finance-bucket"):
    """List all files from GCP bucket grouped by institution"""
    try:
        client = init_gcp_client()
        bucket = client.bucket(bucket_name)
        
        files_by_institution = {}
        blobs = bucket.list_blobs()
        
        for blob in blobs:
            # Parse institution from path
            parts = blob.name.split('/')
            if len(parts) >= 2:
                institution = parts[0]
                filename = '/'.join(parts[1:])
                
                if institution not in files_by_institution:
                    files_by_institution[institution] = []
                
                files_by_institution[institution].append({
                    'filename': filename,
                    'blob_name': blob.name,
                    'size': blob.size,
                    'created': blob.time_created
                })
        
        return files_by_institution
    except Exception as e:
        st.error(f"Failed to list files: {str(e)}")
        return {}

def delete_file_from_gcp(blob_name, bucket_name="your-finance-bucket"):
    """Delete file from GCP bucket"""
    try:
        client = init_gcp_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        return True
    except Exception as e:
        st.error(f"Delete failed: {str(e)}")
        return False

# Streamlit App
st.title('🏦 Personal Finance Dashboard')
st.markdown("Upload and manage your financial institution files")

# Initialize session state for uploaded files tracking
if 'uploaded_files_log' not in st.session_state:
    st.session_state.uploaded_files_log = []

# Create tabs
tab1, tab2 = st.tabs(["📁 Upload Files", "📋 Manage Files"])

with tab1:
    st.header("Upload Financial Data")
    
    # Form for file upload
    with st.form("upload_form"):
        # Financial Institution Dropdown
        institution = st.selectbox(
            "Select Financial Institution",
            ["American Express", "Wealthsimple"],
            index=0
        )
        
        # File uploader
        uploaded_files = st.file_uploader(
            f"Choose CSV files for {institution}", 
            type=['csv'], 
            accept_multiple_files=True,
            key="file_uploader"
        )
        
        # Process button
        process_button = st.form_submit_button("🚀 Process Files", type="primary")
        
        if process_button and uploaded_files:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            uploaded_count = 0
            total_files = len(uploaded_files)
            
            for i, file in enumerate(uploaded_files):
                status_text.text(f"Uploading {file.name}...")
                
                # Upload to GCP
                blob_path = upload_to_gcp(file, institution, file.name)
                
                if blob_path:
                    uploaded_count += 1
                    # Add to session state log
                    st.session_state.uploaded_files_log.append({
                        'institution': institution,
                        'filename': file.name,
                        'blob_path': blob_path,
                        'upload_time': datetime.now(),
                        'size': file.size
                    })
                
                # Update progress
                progress_bar.progress((i + 1) / total_files)
            
            status_text.empty()
            progress_bar.empty()
            
            if uploaded_count > 0:
                st.success(f"✅ Successfully uploaded {uploaded_count}/{total_files} files to {institution}!")
                # Clear the file uploader
                st.rerun()
            else:
                st.error("❌ No files were uploaded successfully")

with tab2:
    st.header("Manage Uploaded Files")
    
    # Refresh button
    if st.button("🔄 Refresh File List"):
        st.rerun()
    
    # Get files from GCP
    files_by_institution = list_files_from_gcp()
    
    if not files_by_institution:
        st.info("No files found in the storage bucket.")
    else:
        for institution, files in files_by_institution.items():
            st.subheader(f"📊 {institution}")
            
            if not files:
                st.write("No files for this institution")
                continue
            
            # Create a DataFrame for better display
            df_data = []
            for file_info in files:
                df_data.append({
                    'Filename': file_info['filename'],
                    'Size (KB)': round(file_info['size'] / 1024, 2),
                    'Uploaded': file_info['created'].strftime("%Y-%m-%d %H:%M:%S") if file_info['created'] else 'Unknown'
                })
            
            df = pd.DataFrame(df_data)
            st.dataframe(df, use_container_width=True)
            
            # Delete functionality
            st.write("**Delete Files:**")
            cols = st.columns([3, 1])
            
            with cols[0]:
                file_to_delete = st.selectbox(
                    "Select file to delete",
                    options=[f['filename'] for f in files],
                    key=f"delete_select_{institution}"
                )
            
            with cols[1]:
                if st.button("🗑️ Delete", key=f"delete_btn_{institution}"):
                    # Find the blob name for the selected file
                    blob_name = None
                    for file_info in files:
                        if file_info['filename'] == file_to_delete:
                            blob_name = file_info['blob_name']
                            break
                    
                    if blob_name and delete_file_from_gcp(blob_name):
                        st.success(f"Deleted {file_to_delete}")
                        st.rerun()
                    else:
                        st.error(f"Failed to delete {file_to_delete}")
            
            st.divider()

# Sidebar with session info
with st.sidebar:
    st.header("📈 Session Summary")
    
    if st.session_state.uploaded_files_log:
        st.write(f"**Files uploaded this session:** {len(st.session_state.uploaded_files_log)}")
        
        # Group by institution
        session_by_institution = {}
        for log_entry in st.session_state.uploaded_files_log:
            inst = log_entry['institution']
            if inst not in session_by_institution:
                session_by_institution[inst] = 0
            session_by_institution[inst] += 1
        
        for inst, count in session_by_institution.items():
            st.write(f"• {inst}: {count} files")
    else:
        st.write("No files uploaded this session")
    
    # Configuration note
    st.divider()
    st.write("**⚙️ Configuration Required:**")
    st.write("• Set up GCP authentication")
    st.write("• Update bucket name in code")
    st.write("• Ensure GCP Storage permissions")


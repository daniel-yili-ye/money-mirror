import streamlit as st
import polars as pl
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import os
from auth import Authenticator


def get_redirect_uri():
    """Get the appropriate redirect URI based on the environment"""
    # Check if we're running in Streamlit Cloud (production)
    # Streamlit Cloud sets various environment variables
    if any(key in os.environ for key in ["STREAMLIT_CLOUD", "STREAMLIT_SHARING"]):
        return st.secrets["deployment"]["production_url"]
    else:
        # Local development
        return st.secrets["deployment"]["local_url"]


# Initialize GCP Storage client
@st.cache_resource
def init_gcp_client():
    """Initialize and cache the GCP storage client using service account from secrets"""

    # Create credentials from the service account info in secrets
    credentials_info = {
        "type": st.secrets["connections"]["gcs"]["type"],
        "project_id": st.secrets["connections"]["gcs"]["project_id"],
        "private_key_id": st.secrets["connections"]["gcs"]["private_key_id"],
        "private_key": st.secrets["connections"]["gcs"]["private_key"],
        "client_email": st.secrets["connections"]["gcs"]["client_email"],
        "client_id": st.secrets["connections"]["gcs"]["client_id"],
        "auth_uri": st.secrets["connections"]["gcs"]["auth_uri"],
        "token_uri": st.secrets["connections"]["gcs"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["connections"]["gcs"][
            "auth_provider_x509_cert_url"
        ],
        "client_x509_cert_url": st.secrets["connections"]["gcs"][
            "client_x509_cert_url"
        ],
        "universe_domain": st.secrets["connections"]["gcs"]["universe_domain"],
    }

    credentials = service_account.Credentials.from_service_account_info(
        credentials_info
    )
    return storage.Client(
        credentials=credentials, project=credentials_info["project_id"]
    )


def upload_to_gcp(
    file_obj, institution, filename, bucket_name="personal-finance-dashboard"
):
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


def list_files_from_gcp(bucket_name="personal-finance-dashboard"):
    """List all files from GCP bucket grouped by institution"""
    try:
        client = init_gcp_client()
        bucket = client.bucket(bucket_name)

        files_by_institution = {}
        blobs = bucket.list_blobs()

        for blob in blobs:
            # Parse institution from path
            parts = blob.name.split("/")
            if len(parts) >= 2:
                institution = parts[0]
                filename = "/".join(parts[1:])

                if institution not in files_by_institution:
                    files_by_institution[institution] = []

                files_by_institution[institution].append(
                    {
                        "filename": filename,
                        "blob_name": blob.name,
                        "size": blob.size,
                        "created": blob.time_created,
                    }
                )

        return files_by_institution
    except Exception as e:
        st.error(f"Failed to list files: {str(e)}")
        return {}


def delete_file_from_gcp(blob_name, bucket_name="personal-finance-dashboard"):
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


def render_file_upload_view():
    """Render the file upload interface"""
    st.header("📁 Upload Financial Data")

    # Form for file upload
    with st.form("upload_form"):
        # Financial Institution Dropdown
        institution = st.selectbox(
            "Select Financial Institution",
            ["American Express", "Wealthsimple"],
            index=0,
        )

        # File uploader
        uploaded_files = st.file_uploader(
            "Choose CSV or Excel files",
            type=["csv", "xls", "xlsx"],
            accept_multiple_files=True,
            key="file_uploader",
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
                    st.session_state.uploaded_files_log.append(
                        {
                            "institution": institution,
                            "filename": file.name,
                            "blob_path": blob_path,
                            "upload_time": datetime.now(),
                            "size": file.size,
                        }
                    )

                # Update progress
                progress_bar.progress((i + 1) / total_files)

            status_text.empty()
            progress_bar.empty()

            if uploaded_count > 0:
                st.success(
                    f"✅ Successfully uploaded {uploaded_count}/{total_files} files to {institution}!"
                )
                # Clear the file uploader
                st.rerun()
            else:
                st.error("❌ No files were uploaded successfully")


def render_file_manager_view():
    """Render the file management interface"""
    st.header("📋 Manage Uploaded Files")

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
                df_data.append(
                    {
                        "Filename": file_info["filename"],
                        "Size (KB)": round(file_info["size"] / 1024, 2),
                        "Uploaded": file_info["created"].strftime("%Y-%m-%d %H:%M:%S")
                        if file_info["created"]
                        else "Unknown",
                    }
                )

            df = pl.DataFrame(df_data)
            st.dataframe(df, use_container_width=True)

            # Delete functionality
            st.write("**Delete Files:**")

            file_to_delete = st.selectbox(
                "Select file to delete",
                options=[f["filename"] for f in files],
                key=f"delete_select_{institution}",
            )

            if st.button("🗑️ Delete", key=f"delete_btn_{institution}"):
                # Find the blob name for the selected file
                blob_name = None
                for file_info in files:
                    if file_info["filename"] == file_to_delete:
                        blob_name = file_info["blob_name"]
                        break

                if blob_name and delete_file_from_gcp(blob_name):
                    st.success(f"Deleted {file_to_delete}")
                    st.rerun()
                else:
                    st.error(f"Failed to delete {file_to_delete}")

            st.divider()


def render_analytics_view():
    """Render the analytics interface"""
    st.header("📊 Financial Analytics")

    # Get files for analysis
    files_by_institution = list_files_from_gcp()

    if not files_by_institution:
        st.warning("No data available for analysis. Please upload some files first.")
        return

    # Summary cards
    total_files = sum(len(files) for files in files_by_institution.values())
    total_institutions = len(files_by_institution)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Files", total_files)
    with col2:
        st.metric("Institutions", total_institutions)
    with col3:
        # Calculate total storage used
        total_size = 0
        for files in files_by_institution.values():
            total_size += sum(f["size"] for f in files)
        st.metric("Storage Used", f"{total_size / (1024 * 1024):.1f} MB")

    st.divider()

    # Institution breakdown
    st.subheader("📈 Data by Institution")

    for institution, files in files_by_institution.items():
        with st.expander(f"{institution} ({len(files)} files)"):
            if files:
                # Create a simple analysis
                total_size = sum(f["size"] for f in files)
                latest_upload = max(f["created"] for f in files if f["created"])
                oldest_upload = min(f["created"] for f in files if f["created"])

                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Total Size:** {total_size / 1024:.1f} KB")
                    st.write(f"**File Count:** {len(files)}")
                with col2:
                    st.write(
                        f"**Latest Upload:** {latest_upload.strftime('%Y-%m-%d %H:%M') if latest_upload else 'N/A'}"
                    )
                    st.write(
                        f"**Oldest Upload:** {oldest_upload.strftime('%Y-%m-%d %H:%M') if oldest_upload else 'N/A'}"
                    )

                # File list
                st.write("**Files:**")
                for file_info in files:
                    st.write(
                        f"• {file_info['filename']} ({file_info['size'] / 1024:.1f} KB)"
                    )

    st.divider()

    # Placeholder for future analytics
    st.subheader("🔮 Coming Soon")
    st.info("""
    **Planned Analytics Features:**
    - 📊 Spending breakdown by category
    - 📈 Monthly spending trends
    - 💡 Personalized financial insights
    - 🎯 Budget recommendations
    - 📱 Export reports
    """)


def render_sidebar_navigation():
    """Render the sidebar navigation"""
    with st.sidebar:
        st.title("🏦 Navigation")

        # Navigation options
        page = st.radio(
            "Choose a view:", ["📁 File Manager", "📊 Analytics"], key="navigation"
        )

        st.divider()

        # User info and logout
        st.write("👤 **User Info**")
        st.write(f"Email: {st.session_state['user_info'].get('email', 'Unknown')}")

        if st.button("🚪 Logout", type="secondary"):
            authenticator.logout()

        st.divider()

        # Session info
        st.write("📊 **Session Stats**")
        files_uploaded = len(st.session_state.get("uploaded_files_log", []))
        st.write(f"Files uploaded: {files_uploaded}")

        return page


# Streamlit App Configuration
st.set_page_config(
    page_title="Personal Finance Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Hide the CookieManager iframe
st.markdown(
    """
<style>
    iframe[title="extra_streamlit_components.CookieManager.cookie_manager"] {
        display: none !important;
    }
    
    /* Also hide any containers with zero height that might take up space */
    div[data-testid="stCustomComponentV1"] iframe[height="0"] {
        display: none !important;
    }
    
    /* Hide the parent container if it only contains the cookie manager */
    .stElementContainer:has(iframe[title*="cookie_manager"]) {
        display: none !important;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Main App
st.title("💰 Personal Finance Dashboard")
st.markdown(
    "Upload your statements to get clear spending breakdowns and personalized recommendations"
)

# Initialize authenticator
authenticator = Authenticator(
    token_key=st.secrets["auth"]["JWT_SECRET_KEY"],
    redirect_uri=get_redirect_uri(),
)
authenticator.check_auth()
authenticator.login()

# Show content that requires login
if st.session_state["connected"]:
    # Initialize session state for uploaded files tracking
    if "uploaded_files_log" not in st.session_state:
        st.session_state.uploaded_files_log = []

    # Render sidebar navigation and get selected page
    selected_page = render_sidebar_navigation()

    # Render content based on navigation selection
    if selected_page == "📁 File Manager":
        # Create tabs for upload and manage
        tab1, tab2 = st.tabs(["⬆️ Upload Files", "📋 Manage Files"])

        with tab1:
            render_file_upload_view()

        with tab2:
            render_file_manager_view()

    elif selected_page == "📊 Analytics":
        render_analytics_view()

if not st.session_state["connected"]:
    st.info("Please log in to access your financial dashboard.")

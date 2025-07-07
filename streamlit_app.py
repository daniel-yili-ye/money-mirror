import streamlit as st
import polars as pl
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import os


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
    return storage.Client(credentials=credentials)


def upload_to_gcp(
    file_obj,
    institution,
    filename,
    bucket_name="personal-finance-dashboard",
):
    """Upload file to GCP bucket with institution-based organization"""
    
    try:
        client = init_gcp_client()
        bucket = client.bucket(bucket_name)

        # Create institution-specific path: institution/timestamp_filename
        blob_path = f"{institution}/{filename}"

        blob = bucket.blob(blob_path)
        file_obj.seek(0)  # Reset file pointer
        blob.upload_from_file(file_obj)

        return blob_path
    except Exception as e:
        st.error(f"Upload failed: {str(e)}")
        return None


def list_files_from_gcp(bucket_name="personal-finance-dashboard"):
    """List files from GCP bucket grouped by institution"""
    
    try:
        client = init_gcp_client()
        bucket = client.bucket(bucket_name)

        files_by_institution = {}

        # List all files in the bucket
        blobs = bucket.list_blobs()

        for blob in blobs:
            # Parse path: institution/filename
            parts = blob.name.split("/")
            if len(parts) >= 2:  # institution/filename
                institution = parts[0]  # First part is institution
                filename = "/".join(parts[1:])  # Rest is filename

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


def kebab_to_display(kebab_name):
    """Convert kebab-case to readable display format"""
    return kebab_name.replace("-", " ").title()


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
    st.header("📁 Upload Bank / Credit Card Statements")

    # Initialize upload counter for dynamic key
    if "upload_counter" not in st.session_state:
        st.session_state.upload_counter = 0

    # Show upload success toast
    if st.session_state.get("show_upload_toast"):
        uploaded_count = st.session_state.get("uploaded_count", 0)
        total_files = st.session_state.get("total_files", 0)
        institution = st.session_state.get("upload_institution", "")
        display_name = kebab_to_display(institution) if institution else ""
        st.toast(
            f"✅ Successfully uploaded {uploaded_count}/{total_files} files to {display_name}!"
        )
        st.session_state["show_upload_toast"] = False
        st.session_state["uploaded_count"] = None
        st.session_state["total_files"] = None
        st.session_state["upload_institution"] = None

    # Form for file upload
    with st.form("upload_form"):
        # Financial Institution Dropdown
        institution = st.selectbox(
            "Select Statement Type",
            ["american-express-credit-card", "wealthsimple-cash"],
            index=0,
            format_func=kebab_to_display,
        )

        # File uploader with dynamic key to clear after successful upload
        uploaded_files = st.file_uploader(
            "Choose CSV or Excel files",
            type=["csv", "xls", "xlsx"],
            accept_multiple_files=True,
            key=f"file_uploader_{st.session_state.upload_counter}",
        )

        # Buttons
        process_button = st.form_submit_button("🚀 Process Files", type="primary")
        clear_button = st.form_submit_button("🗑️ Clear All Files", type="secondary")

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
                st.session_state["uploaded_count"] = uploaded_count
                st.session_state["total_files"] = total_files
                st.session_state["upload_institution"] = institution
                st.session_state["show_upload_toast"] = True

                # Clear the file uploader by incrementing the counter
                st.session_state.upload_counter += 1

                # Brief pause to show success message, then rerun
                st.rerun()
            else:
                st.error("❌ No files were uploaded successfully")

        # Handle clear button
        if clear_button:
            st.session_state.upload_counter += 1
            st.rerun()


def render_file_manager_view():
    """Render the file management interface"""
    st.header("📋 Manage Uploaded Files")

    # Refresh button
    if st.button("🔄 Refresh File List"):
        st.rerun()

    # Get files from GCP for current user
    files_by_institution = list_files_from_gcp()

    if not files_by_institution:
        st.info("No files found in the storage bucket.")
    else:
        for institution, files in files_by_institution.items():
            display_name = kebab_to_display(institution)
            st.subheader(f"📊 {display_name}")

            if not files:
                st.write("No files for this institution")
                continue

            # Add a header row for clarity
            col1, col2, col3, col4 = st.columns([3, 1, 2, 0.5])
            with col1:
                st.markdown("**Filename**")
            with col2:
                st.markdown("**Size**")
            with col3:
                st.markdown("**Uploaded**")
            with col4:
                st.markdown("**Action**")
            st.markdown("---")

            # Display files with individual delete buttons for each row
            for i, file_info in enumerate(files):
                col1, col2, col3, col4 = st.columns([3, 1, 2, 0.5])

                with col1:
                    st.write(file_info["filename"])

                with col2:
                    st.write(f"{round(file_info['size'] / 1024, 2)} KB")

                with col3:
                    upload_time = (
                        file_info["created"].strftime("%Y-%m-%d %H:%M:%S")
                        if file_info["created"]
                        else "Unknown"
                    )
                    st.write(upload_time)

                with col4:
                    # Trash icon button for each row
                    if st.button(
                        "🗑️",
                        key=f"delete_{institution}_{i}",
                        help=f"Delete {file_info['filename']}",
                    ):
                        blob_name = file_info["blob_name"]
                        filename = file_info["filename"]
                        if delete_file_from_gcp(blob_name):
                            st.session_state["deleted_filename"] = filename
                            st.session_state["show_delete_toast"] = True
                            st.rerun()
                        else:
                            st.error(f"Failed to delete {filename}")

            # Show delete success toast
            if st.session_state.get("show_delete_toast"):
                deleted_file = st.session_state.get("deleted_filename", "file")
                st.toast(f"✅ Successfully deleted {deleted_file}")
                st.session_state["show_delete_toast"] = False
                st.session_state["deleted_filename"] = None

            st.divider()


def render_analytics_view():
    """Render the analytics interface"""
    st.header("📊 Financial Analytics")

    # Get files for analysis for current user
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
        display_name = kebab_to_display(institution)
        with st.expander(f"{display_name} ({len(files)} files)"):
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

        # Session info
        st.write("📊 **Session Stats**")
        files_uploaded = len(st.session_state.get("uploaded_files_log", []))
        st.write(f"Files uploaded: {files_uploaded}")

        return page


# Streamlit App Configuration
st.set_page_config(
    page_title="Money Mirror",
    page_icon="🪞",
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
st.title("🪞 Money Mirror")
st.markdown(
    "Upload your statements to get clear spending breakdowns and personalized recommendations"
)

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

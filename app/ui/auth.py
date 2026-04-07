"""
Autenticação simples — usuário/senha via Streamlit secrets.

Configuração em .streamlit/secrets.toml:
    [users]
    DOT = "godot!"
    joao = "outrasenha"
"""

import streamlit as st

_CSS = """
<style>
footer {visibility: hidden;}
#MainMenu {visibility: hidden;}
</style>
"""


def _get_users() -> dict[str, str]:
    try:
        if "users" in st.secrets:
            return dict(st.secrets["users"])
    except Exception:
        pass
    return {}


def is_authenticated() -> bool:
    return st.session_state.get("authenticated", False)


def show_login() -> None:
    if is_authenticated():
        return

    st.set_page_config(
        page_title="DOT · Login",
        page_icon="⬤",
        layout="centered",
    )

    st.markdown(_CSS, unsafe_allow_html=True)

    st.markdown("<div style='height:80px'></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown(
            "<div style='text-align:center;margin-bottom:32px;line-height:1'>"
            "<span style='font-size:2.4rem;font-weight:700;letter-spacing:-1px;"
            "display:block;margin:0;padding:0'>DOT</span>"
            "<span style='color:#929292;font-size:0.95rem;display:block;"
            "margin:4px 0 0 0;padding:0'>Portfolio Management</span>"
            "</div>",
            unsafe_allow_html=True,
        )

        username = st.text_input("Usuário", placeholder="seu usuário", key="login_user")
        password = st.text_input("Senha", type="password", placeholder="••••••••", key="login_pass")

        clicked = st.button("Entrar", use_container_width=True, type="primary")

        if clicked:
            users = _get_users()
            if username in users and users[username] == password:
                st.session_state.authenticated = True
                st.session_state.username = username
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos")

    st.stop()


_SIDEBAR_CSS = """
<style>
[data-testid="stSidebar"] button {
    background-color: transparent !important;
    background: transparent !important;
    border: 1px solid rgba(255,255,255,0.35) !important;
    color: #FFFFFF !important;
}
[data-testid="stSidebar"] button:hover {
    border-color: rgba(255,255,255,0.7) !important;
    color: #FFFFFF !important;
}
</style>
"""


def show_logout_button() -> None:
    # detecta clique no botao Sair via query param
    if st.query_params.get("logout") == "1":
        st.session_state.authenticated = False
        st.session_state.pop("username", None)
        st.query_params.clear()
        st.rerun()

    with st.sidebar:
        st.markdown("---")
        user = st.session_state.get("username", "")
        st.caption(f"👤 {user}")
        st.markdown(
            """
            <a href="?logout=1" style="
                display:block;
                width:100%;
                box-sizing:border-box;
                text-align:center;
                padding:8px 12px;
                border:1px solid rgba(255,255,255,0.4);
                border-radius:6px;
                color:#FFFFFF;
                text-decoration:none;
                font-size:0.875rem;
                font-weight:500;
                margin-top:4px;
            ">Sair</a>
            """,
            unsafe_allow_html=True,
        )

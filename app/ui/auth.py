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
/* Todos os seletores possíveis de botão no Streamlit 1.36+ */
div.stFormSubmitButton > button,
div.stFormSubmitButton button,
div[data-testid="stFormSubmitButton"] > button,
div[data-testid="stFormSubmitButton"] button,
button[data-testid="baseButton-secondaryFormSubmit"],
button[data-testid="baseButton-primaryFormSubmit"],
button[kind="primaryFormSubmit"],
button[kind="secondaryFormSubmit"] {
    background-color: #FA9B5A !important;
    background: #FA9B5A !important;
    color: #FFFFFF !important;
    border: 1px solid #FA9B5A !important;
    font-weight: 600 !important;
}
div.stFormSubmitButton > button:hover,
div[data-testid="stFormSubmitButton"] button:hover {
    background-color: #e8894a !important;
    background: #e8894a !important;
    border-color: #e8894a !important;
}
footer {visibility: hidden;}
#MainMenu {visibility: hidden;}
[data-testid="stForm"] {border: none !important; padding: 0 !important;}
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

        with st.form("login_form"):
            username = st.text_input("Usuário", placeholder="seu usuário")
            password = st.text_input("Senha", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("Entrar", use_container_width=True, type="primary")

        if submitted:
            users = _get_users()
            if username in users and users[username] == password:
                st.session_state.authenticated = True
                st.session_state.username = username
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos")

    st.stop()


def show_logout_button() -> None:
    with st.sidebar:
        st.markdown("---")
        user = st.session_state.get("username", "")
        st.caption(f"👤 {user}")
        if st.button("Sair", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.pop("username", None)
            st.rerun()

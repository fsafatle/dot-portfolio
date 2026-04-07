"""
Autenticação simples — usuário/senha via Streamlit secrets.

Configuração em .streamlit/secrets.toml:
    [users]
    DOT = "godot!"
    joao = "outrasenha"
"""

import streamlit as st


def _get_users() -> dict[str, str]:
    """Retorna {username: password} do secrets.toml."""
    try:
        if "users" in st.secrets:
            return dict(st.secrets["users"])
    except Exception:
        pass
    return {}


def is_authenticated() -> bool:
    return st.session_state.get("authenticated", False)


def show_login() -> None:
    """Renderiza a tela de login. Faz st.stop() se não autenticado."""
    if is_authenticated():
        return

    st.set_page_config(
        page_title="DOT · Login",
        page_icon="⬤",
        layout="centered",
    )

    # Espaço vertical
    st.markdown("<div style='height:80px'></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown(
            "<div style='text-align:center;margin-bottom:32px;line-height:1'>"
            "<span style='font-size:2.4rem;font-weight:700;letter-spacing:-1px;display:block;margin:0;padding:0'>DOT</span>"
            "<span style='color:#929292;font-size:0.95rem;display:block;margin:4px 0 0 0;padding:0'>Portfolio Management</span>"
            "</div>",
            unsafe_allow_html=True,
        )

        username = st.text_input("Usuário", placeholder="seu usuário")
        password = st.text_input("Senha", type="password", placeholder="••••••••")

        if st.button("Entrar", use_container_width=True, type="primary"):
            users = _get_users()
            if username in users and users[username] == password:
                st.session_state.authenticated = True
                st.session_state.username = username
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos")

    st.stop()


def show_logout_button() -> None:
    """Botão de logout no sidebar."""
    with st.sidebar:
        st.markdown("---")
        user = st.session_state.get("username", "")
        st.caption(f"👤 {user}")
        if st.button("Sair", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.pop("username", None)
            st.rerun()

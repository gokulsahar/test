Implementation prompt:
Build a Tomcat 10 (Jakarta EE 9+, jakarta.servlet / jakarta.websocket namespaces) WAR project called streamlit-gateway with the following behavior:
1. Registry file
Path: /opt/streamlit-gateway/apps.txt (configurable via a servlet context-param or env var).
Format: one app per line, name=port, e.g.:
Code
Loaded at startup and re-read on each request (or cached with a file-modified-time check) so I can add/remove apps by editing the file without redeploying — no restart required.
2. HTTP reverse proxy servlet
URL pattern: /streamlit/* (mapped so /streamlit/<name>/... is the incoming path).
Parses <name> from the path, looks up its port in the registry. If not found, return 404 with a clear message.
Forwards the request (method, headers, query string, body) to http://127.0.0.1:<port>/<name>/... using Java 11+ HttpClient.
Streams the response (status, headers, body) back to the client unchanged. Strip hop-by-hop headers (Connection, Transfer-Encoding, etc.) appropriately.
Note: each Streamlit instance must be run with --server.baseUrlPath=<name> so its internal asset URLs match this prefix exactly.
3. WebSocket proxy endpoint
URL pattern: /streamlit/* for the WS upgrade path Streamlit uses (typically ends in _stcore/stream).
Implement as a jakarta.websocket.server.ServerEndpoint with a programmatic/dynamic path, or use Endpoint+ServerEndpointConfig registered in a ServletContextListener so the <name> segment can be parsed dynamically (annotation-based @ServerEndpoint with {name} path param works well here).
On open: look up <name> in the registry, open a client WebSocket connection to ws://127.0.0.1:<port>/<name>/_stcore/stream (or whatever the parsed path is).
Relay messages bidirectionally: browser→gateway→Streamlit and Streamlit→gateway→browser, both text and binary frames.
On close/error on either side, close the other side cleanly.
4. Landing page
/streamlit/ (no name) shows a simple HTML list of all apps currently in the registry, each linking to /streamlit/<name>/, with a basic up/down status check (e.g. attempt a quick TCP connect or HTTP HEAD to 127.0.0.1:<port> to show "running" vs "not running" — since I'm starting these manually, I want to see at a glance which ones I forgot to launch).
5. Project structure
Maven project (pom.xml) targeting Jakarta EE 9+/Tomcat 10, packaged as WAR.
Classes: ProxyServlet.java, StreamlitWebSocketProxy.java, AppRegistry.java (handles reading/parsing/caching apps.txt), GatewayServlet.java or JSP for the landing page.
web.xml or annotation-based config (@WebServlet) — prefer annotations for simplicity.
No database, no build of multiple WARs — single deployable WAR.
6. Error handling
If registry file is missing/unreadable, landing page shows a clear error instead of crashing.
If a port in the registry has nothing listening (app not started yet), proxy returns a 502 with a readable message rather than hanging.
Give me the full source files, pom.xml, and exact steps to build the WAR and deploy it to Tomcat 10's webapps/ folder, plus the exact streamlit run command I should manually use for each app so the baseUrlPath lines up with the proxy
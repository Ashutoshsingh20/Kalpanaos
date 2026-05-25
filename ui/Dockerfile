FROM nginx:1.25-alpine
COPY index.html dashboard.html apps.html qrcode.js /usr/share/nginx/html/
COPY style.css /usr/share/nginx/html/
COPY app.js /usr/share/nginx/html/
COPY install.sh /usr/share/nginx/html/
COPY kalpana* /usr/share/nginx/html/
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1/ || exit 1

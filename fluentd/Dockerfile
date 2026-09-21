FROM fluent/fluentd:v1.16-1

# Usiamo l'utente root temporaneamente per installare i plugin
USER root

# Installiamo il plugin ufficiale per Kafka
RUN apk add --no-cache --update build-base ruby-dev \
    && gem install fluent-plugin-kafka --no-document \
    && apk del build-base ruby-dev \
    && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem

# Torniamo all'utente fluent per questioni di sicurezza
USER fluent
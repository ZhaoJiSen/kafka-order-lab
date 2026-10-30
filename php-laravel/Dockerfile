ARG PHP_BASE_IMAGE=mq-order-lab-php-runtime:local
FROM ${PHP_BASE_IMAGE}
ENV COMPOSER_MAX_PARALLEL_HTTP=1 COMPOSER_IPRESOLVE=4
COPY composer.json composer.lock* ./
RUN composer install --prefer-dist --no-interaction --no-progress --no-scripts
COPY . .
RUN composer dump-autoload --optimize --no-interaction && mkdir -p /data
CMD ["php", "artisan", "serve", "--host=0.0.0.0", "--port=8000"]

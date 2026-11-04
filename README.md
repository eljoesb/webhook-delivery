# Webhook delivery

Servicio HTTP en Rust que registra destinos de webhooks, acepta eventos y los entrega en segundo plano.

Un endpoint es un destino: nombre, URL y un secreto. Un evento es un aviso para ese destino: tipo y cuerpo JSON. La API guarda el evento antes de intentar enviarlo y responde su id. Un worker, en el mismo proceso, hace `POST` al destino, firma el cuerpo con HMAC-SHA256 y guarda cada intento. Si el destino falla, el evento vuelve a quedar pendiente y se reintenta con espera creciente. Al agotar los intentos queda marcado como fallido.

Estados de un evento:

| Estado | Significado |
|---|---|
| `pending` | Guardado, esperando el primer intento o el siguiente reintento. |
| `in_progress` | Un worker lo reclamó y el `POST` está en curso, o el proceso murió antes de anotar el resultado. |
| `delivered` | El destino respondió `2xx`. |
| `failed` | Se alcanzó `MAX_ATTEMPTS` sin un `2xx`. |

La firma viaja en `X-Webhook-Signature` con la forma `sha256=<hex>` y cubre los bytes exactos del cuerpo. También se envían `X-Webhook-Id`, `X-Webhook-Event` y `X-Webhook-Attempt`. El secreto solo aparece en la respuesta de creación del endpoint y no se escribe en los logs.

Si el proceso se cae después de un `POST` exitoso y antes de guardar el resultado, el arrendamiento vence y el evento puede enviarse otra vez. La entrega es al menos una vez. El receptor puede ignorar el duplicado con `X-Webhook-Id`.

## Decisiones

- **Axum** expone la API JSON.
- **SQLx** habla con PostgreSQL usando SQL explícito (`query`, `query_as`) y aplica las migraciones de `migrations/` al arrancar. No usa las macros `query!`, así el proyecto compila sin la base de datos encendida.
- **Reqwest** hace el `POST` saliente. Hay un solo cliente, con timeout y sin seguir redirects: un `302` no cuenta como entrega.
- **hmac** y **sha2** calculan la firma.

## Ejecutar

Hace falta Rust y Docker.

```bash
docker compose up -d
cargo run
```

El servidor escucha en `http://127.0.0.1:3000`. Si no defines `DATABASE_URL`, usa `postgres://postgres:postgres@localhost:5432/webhooks`.

| Variable | Default | Efecto |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/webhooks` | Conexión a Postgres. |
| `MAX_ATTEMPTS` | `5` | Intentos antes de marcar el evento `failed`. |
| `LEASE_SECONDS` | `30` | Tiempo que un evento permanece `in_progress` si el proceso muere a mitad del envío. |
| `POLL_INTERVAL_MS` | `1000` | Pausa del worker cuando no hay eventos vencidos. |
| `HTTP_TIMEOUT_SECONDS` | `10` | Tiempo máximo de cada `POST`. |

La espera entre reintentos es 1 s, 2 s, 4 s, 8 s… y se detiene en 300 s.

## Probar la API a mano

`cargo test` usa la base `webhooks_test`, no `webhooks`. Puedes dejar el servidor corriendo.

En otra terminal, con el servidor corriendo:

```bash
curl -s -i localhost:3000/health
```

Tiene que responder `200` y `{"status":"ok"}`. Comprueba que el proceso está arriba, sin tocar la base de datos.

```bash
curl -s -i localhost:3000/endpoints \
  -H 'content-type: application/json' \
  -d '{"name":"billing","url":"http://127.0.0.1:4000/hook"}'
```

Responde `201`. El cuerpo incluye `id`, `name`, `url`, `secret` y `created_at`. Guarda el `id` y el `secret`: el secreto no vuelve a salir en las consultas. Si no hay nada escuchando en el puerto 4000, el worker intentará entregar y, tras los reintentos, el evento quedará `failed`. Para ver una entrega exitosa, deja un proceso que responda `200` en esa URL.

```bash
curl -s localhost:3000/endpoints
curl -s localhost:3000/endpoints/EL-ENDPOINT
```

Responden `200` con el nombre y la URL, sin el secreto. El listado sale ordenado por fecha de creación.

```bash
curl -s -i localhost:3000/endpoints/EL-ENDPOINT/events \
  -H 'content-type: application/json' \
  -d '{"type":"invoice.paid","payload":{"invoice_id":10}}'
```

Responde `202` con `id` y `"status":"pending"`. El `202` significa que el evento ya está guardado y el envío ocurre después. Aunque pares el proceso en ese momento, el evento sigue en Postgres.

```bash
curl -s localhost:3000/events/EL-EVENTO
curl -s localhost:3000/events/EL-EVENTO/attempts
```

La primera muestra el estado, el tipo, el cuerpo, el número de intentos y las fechas. La segunda es el historial: cada fila tiene el número de intento, si tuvo éxito, el estado HTTP y un recorte de la respuesta. Nada más crearlo, el historial es `[]`. Unos segundos después, con el worker en marcha, aparece el primer intento.

Estas peticiones tienen que fallar y no crear filas:

```bash
curl -s -i localhost:3000/endpoints \
  -H 'content-type: application/json' \
  -d '{"name":" ","url":"ftp://example.com"}'

curl -s -i localhost:3000/endpoints/00000000-0000-0000-0000-000000000000/events \
  -H 'content-type: application/json' \
  -d '{"type":"invoice.paid","payload":{}}'

curl -s -i localhost:3000/events/00000000-0000-0000-0000-000000000000
```

La primera es `400` `invalid_input`: el nombre vacío y una URL que no es `http` o `https` se rechazan antes de escribir. La segunda y la tercera son `404` `not_found`: el UUID es válido, pero no hay endpoint ni evento con ese id. Un id que no es un UUID, como `/events/no-es-un-uuid`, también es `400`: la ruta ni siquiera se busca. Un fallo de Postgres responde `500` `database_error` con un mensaje genérico; el detalle queda en el log.

## Pruebas automáticas

Postgres tiene que estar arriba. La primera ejecución de las pruebas crea `webhooks_test` si todavía no existe. Un volumen nuevo de Docker también la crea con `docker/init-test-db.sql`.

```bash
docker compose up -d
cargo test
```

Para repetir una sola prueba:

```bash
cargo test nombre_de_la_prueba
```

Las pruebas de entrega están en `tests/worker.rs`. Cada una inserta su propio endpoint y su propio evento en `webhooks_test`, y borra esas filas al terminar. Casi todas entregan un id concreto. `process_available_delivers_one_due_event` reclama el evento vencido que haya, así que las pruebas de entrega se turnan y no se quitan las filas entre sí.

### Endpoints

Están en `tests/endpoints.rs`. No abren la base de datos. Comprueban las reglas que se aplican antes del `INSERT`.

- `name_is_trimmed_and_required`. `"  billing  "` se guarda como `billing`. Un nombre vacío o solo espacios es un error. Así un espacio accidental no crea un endpoint en blanco ni uno con otro nombre.
- `url_must_be_absolute_http`. Acepta `http://127.0.0.1:4000/hook` y `https://example.com`. Rechaza `ftp`, un texto que no es URL y `http://` sin host. El worker solo va a hacer `POST` a destinos absolutos HTTP.
- `secret_is_64_hex_chars_and_unique`. El secreto son 32 bytes aleatorios en hexadecimal (64 caracteres). Dos llamadas no devuelven el mismo valor.

### Eventos

Tampoco usan la base de datos. Comprueban el contrato del estado y del tipo.

- `event_type_is_trimmed_and_required`. `"  invoice.paid  "` queda `invoice.paid`. Un tipo vacío se rechaza con `400` y no se inserta el evento.
- `status_serializes_in_snake_case`. El JSON dice `in_progress`, `delivered` y `failed`, no `InProgress`. Es el texto que ve el cliente y el que se escribe en la columna `status`.
- `status_from_db_rejects_unknown_values`. `pending` se lee bien. Un valor fuera de los cuatro estados, como `sent`, es un error interno. La tabla lo impide con un `CHECK`; esta prueba cubre el caso en el que esa garantía no se cumpliera.

### Entrega

- `response_text_is_truncated_on_char_boundaries`. Un texto más largo de 1024 caracteres se corta en 1024, contando caracteres y no bytes. El carácter `á` ocupa más de un byte; cortar por bytes partiría un carácter UTF-8 y Postgres rechazaría el `TEXT`.
- `backoff_grows_and_then_stops`. Tras el intento 1 se espera 1 s, tras el 2 se espera 2 s y tras el 3 se espera 4 s. En el intento 20 la espera es 300 s. La pausa crece para no martillar un destino caído, y tiene tope para no aplazar el reintento varios días.
- `decide_retries_until_the_last_attempt`. Un `2xx` cierra el evento como `delivered`. Un fallo en el intento 1 de 5 lo deja `pending` y programa 1 s. Un fallo en el intento 5 de 5 lo deja `failed` y no programa otro envío. Esta es la regla, sin HTTP ni Postgres.
- `successful_delivery_is_recorded`. Levanta un servidor local que responde `200`, crea un endpoint apuntando a él y entrega el evento una vez. El estado queda `delivered`, hay un intento con éxito y la firma del encabezado coincide con el HMAC del cuerpo recibido. Demuestra el camino feliz y que se firma lo que realmente se envió.
- `process_available_delivers_one_due_event`. Inserta un evento vencido y llama al worker como lo hace el bucle. La primera pasada lo entrega. La segunda no encuentra nada. Demuestra que el worker toma un solo evento vencido y no se queda en un ciclo vacío.
- `failed_delivery_marks_the_event_failed`. El destino responde `500` y el máximo de intentos es 1. Un solo fallo agota el cupo: el evento queda `failed`, el intento tiene `succeeded = false` y el destino recibió una petición.
- `temporary_failure_is_retried_and_then_delivered`. El destino responde primero `500` y después `200`, con un máximo de 5 intentos. Tras el primer envío el estado es `pending` y `next_attempt_at` está en el futuro, así que el worker no lo vuelve a tomar al instante. La prueba adelanta esa fecha para no dormir el segundo, entrega otra vez y el estado final es `delivered`. El historial es el intento 1 fallido y el intento 2 exitoso. El destino recibió dos peticiones.
- `delivery_fails_when_attempts_are_exhausted`. El destino siempre responde `500` y el máximo es 2. Tras el segundo fallo el estado es `failed`, `attempt_count` es 2 y una tercera reclamación no devuelve el evento. Un evento agotado no se sigue enviando.

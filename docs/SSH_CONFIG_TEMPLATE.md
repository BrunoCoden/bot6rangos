# Configuración SSH para OCI

Para configurar el acceso SSH a la instancia OCI, necesitas crear o editar el archivo `~/.ssh/config`.

## Información necesaria:

1. **IP pública o hostname** de la instancia OCI
2. **Usuario SSH** (típicamente `ubuntu` para Ubuntu o `opc` para Oracle Linux)
3. **Clave privada** (ya tienes: `~/.ssh/ssh-key-2025-11-07.key`)

## Configuración recomendada:

Agrega esto a `~/.ssh/config`:

```
Host oci-bot
    HostName TU_IP_O_HOSTNAME_AQUI
    User ubuntu
    IdentityFile ~/.ssh/ssh-key-2025-11-07.key
    StrictHostKeyChecking no
    UserKnownHostsFile ~/.ssh/known_hosts
```

## Uso:

Una vez configurado, podrás conectarte con:
```bash
ssh oci-bot
```

## Verificar conexión:

```bash
ssh -i ~/.ssh/ssh-key-2025-11-07.key ubuntu@TU_IP_O_HOSTNAME
```





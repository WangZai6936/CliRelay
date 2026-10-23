# 公开注册 / Public portal registration

公开注册创建的是**普通门户账号**。账号可以登录用户门户、管理自己的 API Key 和查看用量；注册不会创建租户、后台成员或管理角色。

## 开启

在 `config.yaml` 增加：

```yaml
portal-registration:
  enabled: true
  # 留空时归属系统租户，也可以填写现有业务租户的 UUID。
  tenant-id: ""
  # 建议先在该租户中创建权限档案，再填写其 ID。
  permission-profile-id: "public-signup"
```

`permission-profile-id` 必须属于所配置的租户。权限档案可以限制可用模型、渠道和用量。若留空，采用现有门户账号默认权限，限制值 `0` 表示不限制。

使用项目现有的配置重载机制应用修改，或执行 `docker compose restart cli-proxy-api`。管理认证服务需要已启用并完成数据库初始化。

将 `https://你的域名/login` 分享给用户，作为统一的账号入口。没有账号的用户点击「立即注册」进入 `/register`；注册成功后返回登录页，并自动填入用户名。登录成功后直接进入 `/manage/apikey-lookup` 用户门户，无需重复登录。

登录页和注册页使用「星桥 / StarBridge」品牌，沿用原管理面板的登录卡片、主题、输入框和模型动画；注册页也使用同一套组件。`/login` 和 `/register` 分别转到面板的 `/manage/portal-login` 和 `/manage/portal-register`。用户退出时先注销会话并清理本地状态，再返回 `/login`，不会回到旧门户落地页。密码不会写入浏览器存储或网址。新账号仍在后台「接入与凭证 → 用户账号」管理。

**本版本需要同步构建面板**：Docker 构建会自动应用 `frontend-patches/0001-starbridge-portal.patch` 后构建原 `codeProxy` 源码。独立部署面板时也需要应用该补丁并重新构建，详见 `frontend-patches/README.md`。旧版上游面板不包含新增的登录注册路由，不能只替换后端二进制。

默认不开启公开注册，已有部署升级后保持原行为。将 `enabled` 改回 `false` 即可关闭新注册；已注册账号仍可登录。

## 注册行为

### 登录页模型展示

登录页的「看看支持哪些模型」按钮会打开品牌图标与模型名称列表。管理员可在「系统设置 → 登录页模型展示」添加或移除展示项、修改名称并调整顺序，保存后访客下一次打开弹窗即可读取新列表。列表为空时展示空状态，不会自动推断或公开实际渠道、凭证、租户和内部模型配置。

后台配置通过 `GET/PUT /v0/management/portal-model-showcase` 读写，需要系统租户配置权限；配置保存在现有带版本的数据库设置中，跨节点共享，旧版本写入返回 `409`。匿名接口 `GET /v0/portal/model-showcase` 仅返回 `items: [{name, icon}]`。图标只能选择内置品牌或通用图标，不接受外部图片地址。最多 50 项，名称最多 80 字符。修改展示列表不改变账号的模型调用权限。

可在初始 YAML 中设置 `portal-model-showcase`，如 `[{name: Claude, icon: claude}]`；数据库首次导入后以后台保存的配置为准。默认列表为空。

### 账号规则

- 用户名为 3–48 位英文字母、数字、点、下划线或短横线，以字母或数字开头。忽略首尾空白及大小写，全局唯一；重复返回 `409 username_taken`，不会自动改名。
- 昵称选填，默认使用用户名，最多 128 字节。
- 密码沿用已有规则：12–72 字节，包含大写字母、小写字母和非字母数字字符；仅保存 bcrypt 哈希。页面包含密码确认。
- 账号与默认 API Key 在同一事务内创建，任一步骤失败都会回滚。新用户自己设置密码，无需首次登录强制改密。
- 注册成功后前往门户登录。注册响应不包含密码、密钥或登录令牌。
- 租户与权限档案由服务端配置决定，注册请求不能指定角色、状态、租户或额度。
- 当前是用户名密码注册，不包含邮箱验证或邀请制。

注册请求使用独立的来源 IP 限流：15 分钟最多处理 5 次尝试，第 6 次返回 `429` 和 `Retry-After`；另设日窗口。无论成功、重复还是格式错误均消耗预算。支持现有 Redis 集群限流，遵循已有可信代理与限流豁免设置，不占用登录密码尝试额度。通过反向代理提供服务时，按项目既有方式配置 `trusted-proxies`。

## 接口

### `GET /v0/portal/auth/registration`

无需登录，返回 `enabled`、`password_min_length`、`password_max_bytes`。不公开租户 ID 或默认权限档案。其他前端可以查询这个接口展示注册入口。

### `POST /v0/portal/auth/register`

```json
{
  "username": "alice",
  "display_name": "Alice",
  "password": "Choose-A-Strong-Password!"
}
```

请求体上限 4 KiB，未知字段和尾随 JSON 会被拒绝。成功返回 `201 {"user": {...}}`；之后使用原有 `POST /v0/portal/auth/login` 登录。

主要错误：`400 validation_failed` 或具体密码规则错误、`403 registration_disabled`、`409 username_taken`、`429 too_many_requests`、`503 registration_unavailable`。服务异常不向匿名请求暴露数据库错误。

## 验证

```sh
go test ./internal/enduser ./internal/config ./internal/api/handlers/management ./internal/api/routes
```

设置 `CLIRELAY_POSTGRES_TEST_DSN` 后，`TestPostgresPublicRegistrationLifecycle` 还会在独立测试数据库中验证并发重名注册、登录、认证和门户密钥查询。测试数据库账号需要创建和删除数据库的权限。

## English

Enable `portal-registration.enabled`, optionally choose an existing tenant and permission profile, then share `/login`. The StarBridge login page links to `/register` when signup is enabled. Registration returns users to login with their username prefilled; successful login opens `/manage/apikey-lookup` with the existing portal session format. New users receive ordinary portal accounts and a default API key atomically. No administration roles or tenants are created. Registration is disabled by default and uses a separate IP throttle. The public capabilities endpoint lets independently released frontends discover the feature. Leaving the profile empty uses existing portal account defaults (zero limits mean unlimited).

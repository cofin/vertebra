import { createRemoteJWKSet, jwtVerify } from "jose";
import type { AuthConfig, AuthProviderConfig } from "./config";

export type AuthClaims = Record<string, any>;

export class AuthValidator {
  private jwksCache = new Map<string, ReturnType<typeof createRemoteJWKSet>>();

  constructor(private config: AuthConfig) {}

  async validate(request: Request): Promise<AuthClaims | null> {
    const providers = this.config.providers;
    if (!providers || providers.length === 0) {
      return null;
    }

    for (const provider of providers) {
      const claims = await this.validateProvider(request, provider);
      if (claims) {
        return claims;
      }
    }

    return null;
  }

  private async validateProvider(request: Request, provider: AuthProviderConfig): Promise<AuthClaims | null> {
    try {
      if (provider.type === "iap") {
        const token = request.headers.get("x-goog-iap-jwt-assertion");
        if (!token) return null;

        const jwksUri = "https://www.gstatic.com/iap/verify/public_key-jwk";
        return await this.verifyJwt(token, jwksUri, {
          issuer: "https://cloud.google.com/iap",
          audience: provider.audience,
        });
      }

      if (provider.type === "jwt" || provider.type === "oauth2") {
        let token = this.extractBearerToken(request);
        
        if (!token && this.config.cookieName) {
          token = this.extractCookieToken(request, this.config.cookieName);
        }

        if (!token) return null;

        if (provider.secret) {
          return await this.verifyJwtSymmetric(token, provider.secret, {
            issuer: provider.issuer,
            audience: provider.audience,
          });
        }

        if (!provider.jwksUri) return null;
        return await this.verifyJwt(token, provider.jwksUri, {
          issuer: provider.issuer,
          audience: provider.audience,
        });
      }
    } catch (e) {
      console.error("Auth validation failed:", e);
      // Individual provider failure shouldn't crash the whole validation
      return null;
    }

    return null;
  }

  private extractBearerToken(request: Request): string | null {
    const authHeader = request.headers.get("authorization");
    if (!authHeader || !authHeader.startsWith("Bearer ")) return null;
    return authHeader.substring(7);
  }

  private extractCookieToken(request: Request, cookieName: string): string | null {
    const cookieHeader = request.headers.get("cookie");
    if (!cookieHeader) return null;

    const cookies = cookieHeader.split(";").map(c => c.trim());
    for (const cookie of cookies) {
      if (cookie.startsWith(`${cookieName}=`)) {
        return cookie.substring(cookieName.length + 1);
      }
    }
    return null;
  }

  private async verifyJwt(token: string, jwksUri: string, options: { issuer?: string; audience?: string }): Promise<AuthClaims | null> {
    let jwks = this.jwksCache.get(jwksUri);
    if (!jwks) {
      jwks = createRemoteJWKSet(new URL(jwksUri));
      this.jwksCache.set(jwksUri, jwks);
    }

    const { payload } = await jwtVerify(token, jwks, {
      issuer: options.issuer,
      audience: options.audience,
    });

    return payload;
  }

  private async verifyJwtSymmetric(token: string, secret: string, options: { issuer?: string; audience?: string }): Promise<AuthClaims | null> {
    const secretKey = new TextEncoder().encode(secret);
    const { payload } = await jwtVerify(token, secretKey, {
      issuer: options.issuer,
      audience: options.audience,
    });
    return payload;
  }
}

import { createRemoteJWKSet, jwtVerify, SignJWT, errors } from "jose";
import type { AuthConfig, AuthProviderConfig } from "./config";

export type AuthClaims = Record<string, any>;

export type AuthResult = {
  claims: AuthClaims;
  newTokens?: { accessToken: string; refreshToken?: string };
};

export class AuthValidator {
  private jwksCache = new Map<string, ReturnType<typeof createRemoteJWKSet>>();

  constructor(private config: AuthConfig) {}

  async validate(request: Request): Promise<AuthResult | null> {
    const providers = this.config.providers;
    if (!providers || providers.length === 0) {
      return null;
    }

    for (const provider of providers) {
      const result = await this.validateProvider(request, provider);
      if (result) {
        return result;
      }
    }

    return null;
  }

  private async validateProvider(
    request: Request,
    provider: AuthProviderConfig,
  ): Promise<AuthResult | null> {
    try {
      if (provider.type === "iap") {
        const token = request.headers.get("x-goog-iap-jwt-assertion");
        if (!token) return null;

        const jwksUri = "https://www.gstatic.com/iap/verify/public_key-jwk";
        const claims = await this.verifyJwt(token, jwksUri, {
          issuer: "https://cloud.google.com/iap",
          audience: provider.audience,
        });
        return claims ? { claims } : null;
      }

      if (provider.type === "jwt" || provider.type === "oauth2") {
        let token = this.extractBearerToken(request);

        if (!token && this.config.cookieName) {
          token = this.extractCookieToken(request, this.config.cookieName);
        }

        if (!token) return null;

        if (provider.secret) {
          return await this.verifyJwtSymmetric(token, provider, request);
        }

        if (!provider.jwksUri) return null;
        const claims = await this.verifyJwt(token, provider.jwksUri, {
          issuer: provider.issuer,
          audience: provider.audience,
        });
        return claims ? { claims } : null;
      }
    } catch (e) {
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

    const cookies = cookieHeader.split(";").map((c) => c.trim());
    for (const cookie of cookies) {
      if (cookie.startsWith(`${cookieName}=`)) {
        return cookie.substring(cookieName.length + 1);
      }
    }
    return null;
  }

  private async verifyJwt(
    token: string,
    jwksUri: string,
    options: { issuer?: string; audience?: string },
  ): Promise<AuthClaims | null> {
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

  private async verifyJwtSymmetric(
    token: string,
    provider: AuthProviderConfig,
    request: Request,
  ): Promise<AuthResult | null> {
    if (!provider.secret) return null;
    const secretKey = new TextEncoder().encode(provider.secret);
    
    try {
      const { payload } = await jwtVerify(token, secretKey, {
        issuer: provider.issuer,
        audience: provider.audience,
      });
      return { claims: payload };
    } catch (e) {
      if (e instanceof errors.JWTExpired) {
        // Handle Refresh Token Rotation
        if (this.config.refreshTokenCookieName && provider.refreshSecret) {
           return await this.rotateTokens(request, provider);
        }
      }
      throw e;
    }
  }

  private async rotateTokens(
    request: Request,
    provider: AuthProviderConfig,
  ): Promise<AuthResult | null> {
    if (!this.config.refreshTokenCookieName || !provider.refreshSecret || !provider.secret) {
        return null;
    }

    const refreshToken = this.extractCookieToken(request, this.config.refreshTokenCookieName);
    if (!refreshToken) return null;

    try {
        const refreshSecretKey = new TextEncoder().encode(provider.refreshSecret);
        const { payload } = await jwtVerify(refreshToken, refreshSecretKey, {
            issuer: provider.issuer,
            audience: provider.audience,
        });

        // Ensure it's a refresh token (optional claim check)
        // if (payload.token_type !== 'refresh') return null;

        // Generate new tokens
        // Access Token: 1 hour expiration (default)
        const secretKey = new TextEncoder().encode(provider.secret);
        const newAccessToken = await new SignJWT(payload)
            .setProtectedHeader({ alg: 'HS256' })
            .setIssuedAt()
            .setIssuer(provider.issuer || '')
            .setAudience(provider.audience || '')
            .setExpirationTime('1h')
            .sign(secretKey);

        // Refresh Token: 24h expiration (default) - Rotate it
        // We reuse the claims from the old refresh token (user id, etc)
        const newRefreshToken = await new SignJWT(payload)
            .setProtectedHeader({ alg: 'HS256' })
            .setIssuedAt()
            .setIssuer(provider.issuer || '')
            .setAudience(provider.audience || '')
            .setExpirationTime('24h')
            .sign(refreshSecretKey);

        return {
            claims: payload,
            newTokens: {
                accessToken: newAccessToken,
                refreshToken: newRefreshToken
            }
        };

    } catch (e) {
        // Refresh token invalid or expired
        return null;
    }
  }
}
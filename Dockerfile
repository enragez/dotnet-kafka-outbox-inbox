FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG SERVICE
WORKDIR /source
COPY . .
RUN dotnet publish src/${SERVICE}/${SERVICE}.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG SERVICE
ENV APP_DLL=${SERVICE}.dll
ENV ASPNETCORE_URLS=http://+:8080
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec dotnet \"$APP_DLL\""]

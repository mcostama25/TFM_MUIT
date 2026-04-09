use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get},
    Json, Router,
};

use crate::{
    error::AppError,
    models::common::{Keyword, QualifiedRelation, Reference, Relation, Resource, Theme},
};

use super::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Keywords
        .route("/keywords", get(list_keywords).post(create_keyword))
        .route("/keywords/:keyword_id", delete(delete_keyword))
        // Themes
        .route("/themes", get(list_themes).post(create_theme))
        .route("/themes/:theme_id", delete(delete_theme))
        // References
        .route("/references", get(list_references).post(create_reference))
        .route(
            "/references/:reference_id",
            get(get_reference)
                .put(update_reference)
                .delete(delete_reference),
        )
        // Relations
        .route("/relations", get(list_relations).post(create_relation))
        .route(
            "/relations/:relation_id",
            get(get_relation)
                .put(update_relation)
                .delete(delete_relation),
        )
        .route(
            "/resources/:resource_id/relations",
            get(list_resource_relations),
        )
        // Qualified relations
        .route(
            "/qualified_relations",
            get(list_qualified_relations).post(create_qualified_relation),
        )
        .route(
            "/qualified_relations/:relation_id",
            get(get_qualified_relation)
                .put(update_qualified_relation)
                .delete(delete_qualified_relation),
        )
        .route(
            "/resources/:resource_id/qualified_relations",
            get(list_resource_qualified_relations),
        )
        // Resources
        .route("/resources", get(list_resources).post(create_resource))
        .route(
            "/resources/:resource_id",
            get(get_resource)
                .put(update_resource)
                .delete(delete_resource),
        )
}

// --- Keywords ---

async fn list_keywords(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Keyword>>, AppError> {
    Ok(Json(state.keywords.list().await?))
}

async fn create_keyword(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Keyword>,
) -> Result<(StatusCode, Json<Keyword>), AppError> {
    let created = state.keywords.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn delete_keyword(
    State(state): State<Arc<AppState>>,
    Path(keyword_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.keywords.delete(&keyword_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Themes ---

async fn list_themes(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Theme>>, AppError> {
    Ok(Json(state.themes.list().await?))
}

async fn create_theme(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Theme>,
) -> Result<(StatusCode, Json<Theme>), AppError> {
    let created = state.themes.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn delete_theme(
    State(state): State<Arc<AppState>>,
    Path(theme_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.themes.delete(&theme_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- References ---

async fn list_references(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Reference>>, AppError> {
    Ok(Json(state.references.list().await?))
}

async fn create_reference(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Reference>,
) -> Result<(StatusCode, Json<Reference>), AppError> {
    let created = state.references.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_reference(
    State(state): State<Arc<AppState>>,
    Path(reference_id): Path<String>,
) -> Result<Json<Reference>, AppError> {
    Ok(Json(state.references.get(&reference_id).await?))
}

async fn update_reference(
    State(state): State<Arc<AppState>>,
    Path(reference_id): Path<String>,
    Json(body): Json<Reference>,
) -> Result<StatusCode, AppError> {
    state.references.update(&reference_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_reference(
    State(state): State<Arc<AppState>>,
    Path(reference_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.references.delete(&reference_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Relations ---

async fn list_relations(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Relation>>, AppError> {
    Ok(Json(state.relations.list().await?))
}

async fn create_relation(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Relation>,
) -> Result<(StatusCode, Json<Relation>), AppError> {
    let created = state.relations.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_relation(
    State(state): State<Arc<AppState>>,
    Path(relation_id): Path<String>,
) -> Result<Json<Relation>, AppError> {
    Ok(Json(state.relations.get(&relation_id).await?))
}

async fn update_relation(
    State(state): State<Arc<AppState>>,
    Path(relation_id): Path<String>,
    Json(body): Json<Relation>,
) -> Result<StatusCode, AppError> {
    state.relations.update(&relation_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_relation(
    State(state): State<Arc<AppState>>,
    Path(relation_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.relations.delete(&relation_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_resource_relations(
    State(state): State<Arc<AppState>>,
    Path(resource_id): Path<String>,
) -> Result<Json<Vec<Relation>>, AppError> {
    Ok(Json(
        state.relations.list_for_resource(&resource_id).await?,
    ))
}

// --- Qualified Relations ---

async fn list_qualified_relations(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<QualifiedRelation>>, AppError> {
    Ok(Json(state.qualified_relations.list().await?))
}

async fn create_qualified_relation(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QualifiedRelation>,
) -> Result<(StatusCode, Json<QualifiedRelation>), AppError> {
    let created = state.qualified_relations.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_qualified_relation(
    State(state): State<Arc<AppState>>,
    Path(relation_id): Path<String>,
) -> Result<Json<QualifiedRelation>, AppError> {
    Ok(Json(state.qualified_relations.get(&relation_id).await?))
}

async fn update_qualified_relation(
    State(state): State<Arc<AppState>>,
    Path(relation_id): Path<String>,
    Json(body): Json<QualifiedRelation>,
) -> Result<StatusCode, AppError> {
    state
        .qualified_relations
        .update(&relation_id, body)
        .await?;
    Ok(StatusCode::OK)
}

async fn delete_qualified_relation(
    State(state): State<Arc<AppState>>,
    Path(relation_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.qualified_relations.delete(&relation_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_resource_qualified_relations(
    State(state): State<Arc<AppState>>,
    Path(resource_id): Path<String>,
) -> Result<Json<Vec<QualifiedRelation>>, AppError> {
    Ok(Json(
        state
            .qualified_relations
            .list_for_resource(&resource_id)
            .await?,
    ))
}

// --- Resources ---

async fn list_resources(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<Resource>>, AppError> {
    Ok(Json(state.resources.list().await?))
}

async fn create_resource(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Resource>,
) -> Result<(StatusCode, Json<Resource>), AppError> {
    let created = state.resources.create(body).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_resource(
    State(state): State<Arc<AppState>>,
    Path(resource_id): Path<String>,
) -> Result<Json<Resource>, AppError> {
    Ok(Json(state.resources.get(&resource_id).await?))
}

async fn update_resource(
    State(state): State<Arc<AppState>>,
    Path(resource_id): Path<String>,
    Json(body): Json<Resource>,
) -> Result<StatusCode, AppError> {
    state.resources.update(&resource_id, body).await?;
    Ok(StatusCode::OK)
}

async fn delete_resource(
    State(state): State<Arc<AppState>>,
    Path(resource_id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.resources.delete(&resource_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

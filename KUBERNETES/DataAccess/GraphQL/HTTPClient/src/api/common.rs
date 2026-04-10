use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use tracing::info;

use crate::{
    models::common::{Keyword, QualifiedRelation, Reference, Relation, Resource, Theme},
    repository::common::{
        KeywordRepository, QualifiedRelationRepository, ReferenceRepository, RelationRepository,
        ResourceRepository, ThemeRepository,
    },
};

pub struct CommonRouter<K, TH, REF, REL, QR, RES> {
    keyword_svc: Arc<K>,
    theme_svc: Arc<TH>,
    reference_svc: Arc<REF>,
    relation_svc: Arc<REL>,
    qualified_relation_svc: Arc<QR>,
    resource_svc: Arc<RES>,
}

impl<K, TH, REF, REL, QR, RES> CommonRouter<K, TH, REF, REL, QR, RES>
where
    K: KeywordRepository + Send + Sync + 'static,
    TH: ThemeRepository + Send + Sync + 'static,
    REF: ReferenceRepository + Send + Sync + 'static,
    REL: RelationRepository + Send + Sync + 'static,
    QR: QualifiedRelationRepository + Send + Sync + 'static,
    RES: ResourceRepository + Send + Sync + 'static,
{
    pub fn new(
        keyword_svc: Arc<K>,
        theme_svc: Arc<TH>,
        reference_svc: Arc<REF>,
        relation_svc: Arc<REL>,
        qualified_relation_svc: Arc<QR>,
        resource_svc: Arc<RES>,
    ) -> Self {
        Self { keyword_svc, theme_svc, reference_svc, relation_svc, qualified_relation_svc, resource_svc }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/keywords", get(Self::handle_list_keywords).post(Self::handle_create_keyword))
            .route("/keywords/:keyword_id", axum::routing::delete(Self::handle_delete_keyword))
            .route("/themes", get(Self::handle_list_themes).post(Self::handle_create_theme))
            .route("/themes/:theme_id", axum::routing::delete(Self::handle_delete_theme))
            .route(
                "/references",
                get(Self::handle_list_references).post(Self::handle_create_reference),
            )
            .route(
                "/references/:reference_id",
                get(Self::handle_get_reference)
                    .put(Self::handle_update_reference)
                    .delete(Self::handle_delete_reference),
            )
            .route(
                "/relations",
                get(Self::handle_list_relations).post(Self::handle_create_relation),
            )
            .route(
                "/relations/:relation_id",
                get(Self::handle_get_relation)
                    .put(Self::handle_update_relation)
                    .delete(Self::handle_delete_relation),
            )
            .route("/resources/:resource_id/relations", get(Self::handle_list_resource_relations))
            .route(
                "/qualified_relations",
                get(Self::handle_list_qualified_relations).post(Self::handle_create_qualified_relation),
            )
            .route(
                "/qualified_relations/:relation_id",
                get(Self::handle_get_qualified_relation)
                    .put(Self::handle_update_qualified_relation)
                    .delete(Self::handle_delete_qualified_relation),
            )
            .route(
                "/resources/:resource_id/qualified_relations",
                get(Self::handle_list_resource_qualified_relations),
            )
            .route(
                "/resources",
                get(Self::handle_list_resources).post(Self::handle_create_resource),
            )
            .route(
                "/resources/:resource_id",
                get(Self::handle_get_resource)
                    .put(Self::handle_update_resource)
                    .delete(Self::handle_delete_resource),
            )
            .with_state((
                self.keyword_svc,
                self.theme_svc,
                self.reference_svc,
                self.relation_svc,
                self.qualified_relation_svc,
                self.resource_svc,
            ))
    }

    async fn handle_list_keywords(
        State((kw, _, _, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
    ) -> impl IntoResponse {
        info!("GET /keywords");
        match kw.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_keyword(
        State((kw, _, _, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Json(body): Json<Keyword>,
    ) -> impl IntoResponse {
        info!("POST /keywords");
        match kw.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_keyword(
        State((kw, _, _, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(keyword_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /keywords/{keyword_id}");
        match kw.delete(&keyword_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_themes(
        State((_, th, _, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
    ) -> impl IntoResponse {
        info!("GET /themes");
        match th.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_theme(
        State((_, th, _, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Json(body): Json<Theme>,
    ) -> impl IntoResponse {
        info!("POST /themes");
        match th.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_theme(
        State((_, th, _, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(theme_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /themes/{theme_id}");
        match th.delete(&theme_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_references(
        State((_, _, rref, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
    ) -> impl IntoResponse {
        info!("GET /references");
        match rref.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_reference(
        State((_, _, rref, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Json(body): Json<Reference>,
    ) -> impl IntoResponse {
        info!("POST /references");
        match rref.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_reference(
        State((_, _, rref, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(reference_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /references/{reference_id}");
        match rref.get(&reference_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_reference(
        State((_, _, rref, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(reference_id): Path<String>,
        Json(body): Json<Reference>,
    ) -> impl IntoResponse {
        info!("PUT /references/{reference_id}");
        match rref.update(&reference_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_reference(
        State((_, _, rref, _, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(reference_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /references/{reference_id}");
        match rref.delete(&reference_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_relations(
        State((_, _, _, rel, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
    ) -> impl IntoResponse {
        info!("GET /relations");
        match rel.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_relation(
        State((_, _, _, rel, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Json(body): Json<Relation>,
    ) -> impl IntoResponse {
        info!("POST /relations");
        match rel.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_relation(
        State((_, _, _, rel, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(relation_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /relations/{relation_id}");
        match rel.get(&relation_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_relation(
        State((_, _, _, rel, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(relation_id): Path<String>,
        Json(body): Json<Relation>,
    ) -> impl IntoResponse {
        info!("PUT /relations/{relation_id}");
        match rel.update(&relation_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_relation(
        State((_, _, _, rel, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(relation_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /relations/{relation_id}");
        match rel.delete(&relation_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_resource_relations(
        State((_, _, _, rel, _, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(resource_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /resources/{resource_id}/relations");
        match rel.list_for_resource(&resource_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_qualified_relations(
        State((_, _, _, _, qr, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
    ) -> impl IntoResponse {
        info!("GET /qualified_relations");
        match qr.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_qualified_relation(
        State((_, _, _, _, qr, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Json(body): Json<QualifiedRelation>,
    ) -> impl IntoResponse {
        info!("POST /qualified_relations");
        match qr.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_qualified_relation(
        State((_, _, _, _, qr, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(relation_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /qualified_relations/{relation_id}");
        match qr.get(&relation_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_qualified_relation(
        State((_, _, _, _, qr, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(relation_id): Path<String>,
        Json(body): Json<QualifiedRelation>,
    ) -> impl IntoResponse {
        info!("PUT /qualified_relations/{relation_id}");
        match qr.update(&relation_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_qualified_relation(
        State((_, _, _, _, qr, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(relation_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /qualified_relations/{relation_id}");
        match qr.delete(&relation_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_resource_qualified_relations(
        State((_, _, _, _, qr, _)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(resource_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /resources/{resource_id}/qualified_relations");
        match qr.list_for_resource(&resource_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_list_resources(
        State((_, _, _, _, _, res)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
    ) -> impl IntoResponse {
        info!("GET /resources");
        match res.list().await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_create_resource(
        State((_, _, _, _, _, res)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Json(body): Json<Resource>,
    ) -> impl IntoResponse {
        info!("POST /resources");
        match res.create(body).await {
            Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_get_resource(
        State((_, _, _, _, _, res)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(resource_id): Path<String>,
    ) -> impl IntoResponse {
        info!("GET /resources/{resource_id}");
        match res.get(&resource_id).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_update_resource(
        State((_, _, _, _, _, res)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(resource_id): Path<String>,
        Json(body): Json<Resource>,
    ) -> impl IntoResponse {
        info!("PUT /resources/{resource_id}");
        match res.update(&resource_id, body).await {
            Ok(()) => StatusCode::OK.into_response(),
            Err(e) => e.into_response(),
        }
    }

    async fn handle_delete_resource(
        State((_, _, _, _, _, res)): State<(Arc<K>, Arc<TH>, Arc<REF>, Arc<REL>, Arc<QR>, Arc<RES>)>,
        Path(resource_id): Path<String>,
    ) -> impl IntoResponse {
        info!("DELETE /resources/{resource_id}");
        match res.delete(&resource_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => e.into_response(),
        }
    }
}

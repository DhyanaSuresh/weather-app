var kD=Object.defineProperty,FD=Object.defineProperties;var PD=Object.getOwnPropertyDescriptors;var vh=Object.getOwnPropertySymbols;var LD=Object.prototype.hasOwnProperty,jD=Object.prototype.propertyIsEnumerable;var yh=(e,n,t)=>n in e?kD(e,n,{enumerable:!0,configurable:!0,writable:!0,value:t}):e[n]=t,v=(e,n)=>{for(var t in n||={})LD.call(n,t)&&yh(e,t,n[t]);if(vh)for(var t of vh(n))jD.call(n,t)&&yh(e,t,n[t]);return e},P=(e,n)=>FD(e,PD(n));var Re=null,Ds=!1,jl=1,VD=null,be=Symbol("SIGNAL");function A(e){let n=Re;return Re=e,n}function Is(){return Re}var Wn={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function qn(e){if(Ds)throw new Error("");if(Re===null)return;Re.consumerOnSignalRead(e);let n=Re.producersTail;if(n!==void 0&&n.producer===e)return;let t,r=Re.recomputing;if(r&&(t=n!==void 0?n.nextProducer:Re.producers,t!==void 0&&t.producer===e)){Re.producersTail=t,t.lastReadVersion=e.version;return}let i=e.consumersTail;if(i!==void 0&&i.consumer===Re&&(!r||HD(i,Re)))return;let o=Fr(Re),s={producer:e,consumer:Re,nextProducer:t,prevConsumer:i,lastReadVersion:e.version,nextConsumer:void 0};Re.producersTail=s,n!==void 0?n.nextProducer=s:Re.producers=s,o&&Ch(e,s)}function bh(){jl++}function xs(e){if(!(Fr(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===jl)){if(!e.producerMustRecompute(e)&&!kr(e)){ws(e);return}e.producerRecomputeValue(e),ws(e)}}function Vl(e){if(e.consumers===void 0)return;let n=Ds;Ds=!0;try{for(let t=e.consumers;t!==void 0;t=t.nextConsumer){let r=t.consumer;r.dirty||BD(r)}}finally{Ds=n}}function Bl(){return Re?.consumerAllowSignalWrites!==!1}function BD(e){e.dirty=!0,Vl(e),e.consumerMarkedDirty?.(e)}function ws(e){e.dirty=!1,e.lastCleanEpoch=jl}function vn(e){return e&&_h(e),A(e)}function _h(e){e.producersTail=void 0,e.recomputing=!0}function Zn(e,n){A(n),e&&Dh(e)}function Dh(e){e.recomputing=!1;let n=e.producersTail,t=n!==void 0?n.nextProducer:e.producers;if(t!==void 0){if(Fr(e))do t=Hl(t);while(t!==void 0);n!==void 0?n.nextProducer=void 0:e.producers=void 0}}function kr(e){for(let n=e.producers;n!==void 0;n=n.nextProducer){let t=n.producer,r=n.lastReadVersion;if(r!==t.version||(xs(t),r!==t.version))return!0}return!1}function yn(e){if(Fr(e)){let n=e.producers;for(;n!==void 0;)n=Hl(n)}e.producers=void 0,e.producersTail=void 0,e.consumers=void 0,e.consumersTail=void 0}function Ch(e,n){let t=e.consumersTail,r=Fr(e);if(t!==void 0?(n.nextConsumer=t.nextConsumer,t.nextConsumer=n):(n.nextConsumer=void 0,e.consumers=n),n.prevConsumer=t,e.consumersTail=n,!r)for(let i=e.producers;i!==void 0;i=i.nextProducer)Ch(i.producer,i)}function Hl(e){let n=e.producer,t=e.nextProducer,r=e.nextConsumer,i=e.prevConsumer;if(e.nextConsumer=void 0,e.prevConsumer=void 0,r!==void 0?r.prevConsumer=i:n.consumersTail=i,i!==void 0)i.nextConsumer=r;else if(n.consumers=r,!Fr(n)){let o=n.producers;for(;o!==void 0;)o=Hl(o)}return t}function Fr(e){return e.consumerIsAlwaysLive||e.consumers!==void 0}function Ms(e){VD?.(e)}function HD(e,n){let t=n.producersTail;if(t!==void 0){let r=n.producers;do{if(r===e)return!0;if(r===t)break;r=r.nextProducer}while(r!==void 0)}return!1}function Ss(e,n){return Object.is(e,n)}function Hi(e,n){let t=Object.create(UD);t.computation=e,n!==void 0&&(t.equal=n);let r=()=>{if(xs(t),qn(t),t.value===Bi)throw t.error;return t.value};return r[be]=t,Ms(t),r}var Cs=Symbol("UNSET"),Es=Symbol("COMPUTING"),Bi=Symbol("ERRORED"),UD=P(v({},Wn),{value:Cs,dirty:!0,error:null,equal:Ss,kind:"computed",producerMustRecompute(e){return e.value===Cs||e.value===Es},producerRecomputeValue(e){if(e.value===Es)throw new Error("");let n=e.value;e.value=Es;let t=vn(e),r,i=!1;try{r=e.computation(),A(null),i=n!==Cs&&n!==Bi&&r!==Bi&&e.equal(n,r)}catch(o){r=Bi,e.error=o}finally{Zn(e,t)}if(i){e.value=n;return}e.value=r,e.version++}});function $D(){throw new Error}var Eh=$D;function wh(e){Eh(e)}function Ul(e){Eh=e}var zD=null;function $l(e,n){let t=Object.create(Ui);t.value=e,n!==void 0&&(t.equal=n);let r=()=>Ih(t);return r[be]=t,Ms(t),[r,s=>Pr(t,s),s=>zl(t,s)]}function Ih(e){return qn(e),e.value}function Pr(e,n){Bl()||wh(e),e.equal(e.value,n)||(e.value=n,GD(e))}function zl(e,n){Bl()||wh(e),Pr(e,n(e.value))}var Ui=P(v({},Wn),{equal:Ss,value:void 0,kind:"signal"});function GD(e){e.version++,bh(),Vl(e),zD?.(e)}var Gl=P(v({},Wn),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Wl(e){if(e.dirty=!1,e.version>0&&!kr(e))return;e.version++;let n=vn(e);try{e.cleanup(),e.fn()}finally{Zn(e,n)}}function O(e){return typeof e=="function"}function Lr(e){let t=e(r=>{Error.call(r),r.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var Ts=Lr(e=>function(t){e(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((r,i)=>`${i+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Qn(e,n){if(e){let t=e.indexOf(n);0<=t&&e.splice(t,1)}}var pe=class e{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:r}=this;if(O(r))try{r()}catch(o){n=o instanceof Ts?o.errors:[o]}let{_finalizers:i}=this;if(i){this._finalizers=null;for(let o of i)try{xh(o)}catch(s){n=n??[],s instanceof Ts?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Ts(n)}}add(n){var t;if(n&&n!==this)if(this.closed)xh(n);else{if(n instanceof e){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(n)}}_hasParent(n){let{_parentage:t}=this;return t===n||Array.isArray(t)&&t.includes(n)}_addParent(n){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(n),t):t?[t,n]:n}_removeParent(n){let{_parentage:t}=this;t===n?this._parentage=null:Array.isArray(t)&&Qn(t,n)}remove(n){let{_finalizers:t}=this;t&&Qn(t,n),n instanceof e&&n._removeParent(this)}};pe.EMPTY=(()=>{let e=new pe;return e.closed=!0,e})();var ql=pe.EMPTY;function As(e){return e instanceof pe||e&&"closed"in e&&O(e.remove)&&O(e.add)&&O(e.unsubscribe)}function xh(e){O(e)?e():e.unsubscribe()}var ht={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var jr={setTimeout(e,n,...t){let{delegate:r}=jr;return r?.setTimeout?r.setTimeout(e,n,...t):setTimeout(e,n,...t)},clearTimeout(e){let{delegate:n}=jr;return(n?.clearTimeout||clearTimeout)(e)},delegate:void 0};function Rs(e){jr.setTimeout(()=>{let{onUnhandledError:n}=ht;if(n)n(e);else throw e})}function $i(){}var Mh=Zl("C",void 0,void 0);function Sh(e){return Zl("E",void 0,e)}function Th(e){return Zl("N",e,void 0)}function Zl(e,n,t){return{kind:e,value:n,error:t}}var Yn=null;function Vr(e){if(ht.useDeprecatedSynchronousErrorHandling){let n=!Yn;if(n&&(Yn={errorThrown:!1,error:null}),e(),n){let{errorThrown:t,error:r}=Yn;if(Yn=null,t)throw r}}else e()}function Ah(e){ht.useDeprecatedSynchronousErrorHandling&&Yn&&(Yn.errorThrown=!0,Yn.error=e)}var Kn=class extends pe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,As(n)&&n.add(this)):this.destination=ZD}static create(n,t,r){return new Kt(n,t,r)}next(n){this.isStopped?Yl(Th(n),this):this._next(n)}error(n){this.isStopped?Yl(Sh(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Yl(Mh,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},WD=Function.prototype.bind;function Ql(e,n){return WD.call(e,n)}var Kl=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:t}=this;if(t.next)try{t.next(n)}catch(r){Ns(r)}}error(n){let{partialObserver:t}=this;if(t.error)try{t.error(n)}catch(r){Ns(r)}else Ns(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(t){Ns(t)}}},Kt=class extends Kn{constructor(n,t,r){super();let i;if(O(n)||!n)i={next:n??void 0,error:t??void 0,complete:r??void 0};else{let o;this&&ht.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),i={next:n.next&&Ql(n.next,o),error:n.error&&Ql(n.error,o),complete:n.complete&&Ql(n.complete,o)}):i=n}this.destination=new Kl(i)}};function Ns(e){ht.useDeprecatedSynchronousErrorHandling?Ah(e):Rs(e)}function qD(e){throw e}function Yl(e,n){let{onStoppedNotification:t}=ht;t&&jr.setTimeout(()=>t(e,n))}var ZD={closed:!0,next:$i,error:qD,complete:$i};var Br=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Ue(e){return e}function Xl(...e){return Jl(e)}function Jl(e){return e.length===0?Ue:e.length===1?e[0]:function(t){return e.reduce((r,i)=>i(r),t)}}var j=(()=>{class e{constructor(t){t&&(this._subscribe=t)}lift(t){let r=new e;return r.source=this,r.operator=t,r}subscribe(t,r,i){let o=YD(t)?t:new Kt(t,r,i);return Vr(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(r){t.error(r)}}forEach(t,r){return r=Rh(r),new r((i,o)=>{let s=new Kt({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:i});this.subscribe(s)})}_subscribe(t){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(t)}[Br](){return this}pipe(...t){return Jl(t)(this)}toPromise(t){return t=Rh(t),new t((r,i)=>{let o;this.subscribe(s=>o=s,s=>i(s),()=>r(o))})}}return e.create=n=>new e(n),e})();function Rh(e){var n;return(n=e??ht.Promise)!==null&&n!==void 0?n:Promise}function QD(e){return e&&O(e.next)&&O(e.error)&&O(e.complete)}function YD(e){return e&&e instanceof Kn||QD(e)&&As(e)}function KD(e){return O(e?.lift)}function V(e){return n=>{if(KD(n))return n.lift(function(t){try{return e(t,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function B(e,n,t,r,i){return new ed(e,n,t,r,i)}var ed=class extends Kn{constructor(n,t,r,i,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){n.error(c)}}:super._next,this._error=i?function(a){try{i(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Nh=Lr(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var H=(()=>{class e extends j{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let r=new Os(this,this);return r.operator=t,r}_throwIfClosed(){if(this.closed)throw new Nh}next(t){Vr(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(t)}})}error(t){Vr(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:r}=this;for(;r.length;)r.shift().error(t)}})}complete(){Vr(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:r,isStopped:i,observers:o}=this;return r||i?ql:(this.currentObservers=null,o.push(t),new pe(()=>{this.currentObservers=null,Qn(o,t)}))}_checkFinalizedStatuses(t){let{hasError:r,thrownError:i,isStopped:o}=this;r?t.error(i):o&&t.complete()}asObservable(){let t=new j;return t.source=this,t}}return e.create=(n,t)=>new Os(n,t),e})(),Os=class extends H{constructor(n,t){super(),this.destination=n,this.source=t}next(n){var t,r;(r=(t=this.destination)===null||t===void 0?void 0:t.next)===null||r===void 0||r.call(t,n)}error(n){var t,r;(r=(t=this.destination)===null||t===void 0?void 0:t.error)===null||r===void 0||r.call(t,n)}complete(){var n,t;(t=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||t===void 0||t.call(n)}_subscribe(n){var t,r;return(r=(t=this.source)===null||t===void 0?void 0:t.subscribe(n))!==null&&r!==void 0?r:ql}};var _e=class extends H{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let t=super._subscribe(n);return!t.closed&&n.next(this._value),t}getValue(){let{hasError:n,thrownError:t,_value:r}=this;if(n)throw t;return this._throwIfClosed(),r}next(n){super.next(this._value=n)}};var zi={now(){return(zi.delegate||Date).now()},delegate:void 0};var ks=class extends H{constructor(n=1/0,t=1/0,r=zi){super(),this._bufferSize=n,this._windowTime=t,this._timestampProvider=r,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=t===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,t)}next(n){let{isStopped:t,_buffer:r,_infiniteTimeWindow:i,_timestampProvider:o,_windowTime:s}=this;t||(r.push(n),!i&&r.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let t=this._innerSubscribe(n),{_infiniteTimeWindow:r,_buffer:i}=this,o=i.slice();for(let s=0;s<o.length&&!n.closed;s+=r?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),t}_trimBuffer(){let{_bufferSize:n,_timestampProvider:t,_buffer:r,_infiniteTimeWindow:i}=this,o=(i?1:2)*n;if(n<1/0&&o<r.length&&r.splice(0,r.length-o),!i){let s=t.now(),a=0;for(let c=1;c<r.length&&r[c]<=s;c+=2)a=c;a&&r.splice(0,a+1)}}};var Fs=class extends pe{constructor(n,t){super()}schedule(n,t=0){return this}};var Gi={setInterval(e,n,...t){let{delegate:r}=Gi;return r?.setInterval?r.setInterval(e,n,...t):setInterval(e,n,...t)},clearInterval(e){let{delegate:n}=Gi;return(n?.clearInterval||clearInterval)(e)},delegate:void 0};var Ps=class extends Fs{constructor(n,t){super(n,t),this.scheduler=n,this.work=t,this.pending=!1}schedule(n,t=0){var r;if(this.closed)return this;this.state=n;let i=this.id,o=this.scheduler;return i!=null&&(this.id=this.recycleAsyncId(o,i,t)),this.pending=!0,this.delay=t,this.id=(r=this.id)!==null&&r!==void 0?r:this.requestAsyncId(o,this.id,t),this}requestAsyncId(n,t,r=0){return Gi.setInterval(n.flush.bind(n,this),r)}recycleAsyncId(n,t,r=0){if(r!=null&&this.delay===r&&this.pending===!1)return t;t!=null&&Gi.clearInterval(t)}execute(n,t){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let r=this._execute(n,t);if(r)return r;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,t){let r=!1,i;try{this.work(n)}catch(o){r=!0,i=o||new Error("Scheduled action threw falsy error")}if(r)return this.unsubscribe(),i}unsubscribe(){if(!this.closed){let{id:n,scheduler:t}=this,{actions:r}=t;this.work=this.state=this.scheduler=null,this.pending=!1,Qn(r,this),n!=null&&(this.id=this.recycleAsyncId(t,n,null)),this.delay=null,super.unsubscribe()}}};var Hr=class e{constructor(n,t=e.now){this.schedulerActionCtor=n,this.now=t}schedule(n,t=0,r){return new this.schedulerActionCtor(this,n).schedule(r,t)}};Hr.now=zi.now;var Ls=class extends Hr{constructor(n,t=Hr.now){super(n,t),this.actions=[],this._active=!1}flush(n){let{actions:t}=this;if(this._active){t.push(n);return}let r;this._active=!0;do if(r=n.execute(n.state,n.delay))break;while(n=t.shift());if(this._active=!1,r){for(;n=t.shift();)n.unsubscribe();throw r}}};var XD=new Ls(Ps),Oh=XD;var De=new j(e=>e.complete());function js(e){return e&&O(e.schedule)}function td(e){return e[e.length-1]}function Vs(e){return O(td(e))?e.pop():void 0}function Ft(e){return js(td(e))?e.pop():void 0}function kh(e,n){return typeof td(e)=="number"?e.pop():n}function Ph(e,n,t,r){function i(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(d){try{l(r.next(d))}catch(u){s(u)}}function c(d){try{l(r.throw(d))}catch(u){s(u)}}function l(d){d.done?o(d.value):i(d.value).then(a,c)}l((r=r.apply(e,n||[])).next())})}function Fh(e){var n=typeof Symbol=="function"&&Symbol.iterator,t=n&&e[n],r=0;if(t)return t.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&r>=e.length&&(e=void 0),{value:e&&e[r++],done:!e}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Xn(e){return this instanceof Xn?(this.v=e,this):new Xn(e)}function Lh(e,n,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=t.apply(e,n||[]),i,o=[];return i=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),i[Symbol.asyncIterator]=function(){return this},i;function s(h){return function(g){return Promise.resolve(g).then(h,u)}}function a(h,g){r[h]&&(i[h]=function(I){return new Promise(function(D,w){o.push([h,I,D,w])>1||c(h,I)})},g&&(i[h]=g(i[h])))}function c(h,g){try{l(r[h](g))}catch(I){p(o[0][3],I)}}function l(h){h.value instanceof Xn?Promise.resolve(h.value.v).then(d,u):p(o[0][2],h)}function d(h){c("next",h)}function u(h){c("throw",h)}function p(h,g){h(g),o.shift(),o.length&&c(o[0][0],o[0][1])}}function jh(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=e[Symbol.asyncIterator],t;return n?n.call(e):(e=typeof Fh=="function"?Fh(e):e[Symbol.iterator](),t={},r("next"),r("throw"),r("return"),t[Symbol.asyncIterator]=function(){return this},t);function r(o){t[o]=e[o]&&function(s){return new Promise(function(a,c){s=e[o](s),i(a,c,s.done,s.value)})}}function i(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Bs=e=>e&&typeof e.length=="number"&&typeof e!="function";function Hs(e){return O(e?.then)}function Us(e){return O(e[Br])}function $s(e){return Symbol.asyncIterator&&O(e?.[Symbol.asyncIterator])}function zs(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function JD(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Gs=JD();function Ws(e){return O(e?.[Gs])}function qs(e){return Lh(this,arguments,function*(){let t=e.getReader();try{for(;;){let{value:r,done:i}=yield Xn(t.read());if(i)return yield Xn(void 0);yield yield Xn(r)}}finally{t.releaseLock()}})}function Zs(e){return O(e?.getReader)}function ne(e){if(e instanceof j)return e;if(e!=null){if(Us(e))return eC(e);if(Bs(e))return tC(e);if(Hs(e))return nC(e);if($s(e))return Vh(e);if(Ws(e))return rC(e);if(Zs(e))return iC(e)}throw zs(e)}function eC(e){return new j(n=>{let t=e[Br]();if(O(t.subscribe))return t.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function tC(e){return new j(n=>{for(let t=0;t<e.length&&!n.closed;t++)n.next(e[t]);n.complete()})}function nC(e){return new j(n=>{e.then(t=>{n.closed||(n.next(t),n.complete())},t=>n.error(t)).then(null,Rs)})}function rC(e){return new j(n=>{for(let t of e)if(n.next(t),n.closed)return;n.complete()})}function Vh(e){return new j(n=>{oC(e,n).catch(t=>n.error(t))})}function iC(e){return Vh(qs(e))}function oC(e,n){var t,r,i,o;return Ph(this,void 0,void 0,function*(){try{for(t=jh(e);r=yield t.next(),!r.done;){let s=r.value;if(n.next(s),n.closed)return}}catch(s){i={error:s}}finally{try{r&&!r.done&&(o=t.return)&&(yield o.call(t))}finally{if(i)throw i.error}}n.complete()})}function Ye(e,n,t,r=0,i=!1){let o=n.schedule(function(){t(),i?e.add(this.schedule(null,r)):this.unsubscribe()},r);if(e.add(o),!i)return o}function Qs(e,n=0){return V((t,r)=>{t.subscribe(B(r,i=>Ye(r,e,()=>r.next(i),n),()=>Ye(r,e,()=>r.complete(),n),i=>Ye(r,e,()=>r.error(i),n)))})}function Ys(e,n=0){return V((t,r)=>{r.add(e.schedule(()=>t.subscribe(r),n))})}function Bh(e,n){return ne(e).pipe(Ys(n),Qs(n))}function Hh(e,n){return ne(e).pipe(Ys(n),Qs(n))}function Uh(e,n){return new j(t=>{let r=0;return n.schedule(function(){r===e.length?t.complete():(t.next(e[r++]),t.closed||this.schedule())})})}function $h(e,n){return new j(t=>{let r;return Ye(t,n,()=>{r=e[Gs](),Ye(t,n,()=>{let i,o;try{({value:i,done:o}=r.next())}catch(s){t.error(s);return}o?t.complete():t.next(i)},0,!0)}),()=>O(r?.return)&&r.return()})}function Ks(e,n){if(!e)throw new Error("Iterable cannot be null");return new j(t=>{Ye(t,n,()=>{let r=e[Symbol.asyncIterator]();Ye(t,n,()=>{r.next().then(i=>{i.done?t.complete():t.next(i.value)})},0,!0)})})}function zh(e,n){return Ks(qs(e),n)}function Gh(e,n){if(e!=null){if(Us(e))return Bh(e,n);if(Bs(e))return Uh(e,n);if(Hs(e))return Hh(e,n);if($s(e))return Ks(e,n);if(Ws(e))return $h(e,n);if(Zs(e))return zh(e,n)}throw zs(e)}function de(e,n){return n?Gh(e,n):ne(e)}function M(...e){let n=Ft(e);return de(e,n)}function Jn(e,n){let t=O(e)?e:()=>e,r=i=>i.error(t());return new j(n?i=>n.schedule(r,0,i):r)}function Xs(e){return!!e&&(e instanceof j||O(e.lift)&&O(e.subscribe))}var er=Lr(e=>function(){e(this),this.name="EmptyError",this.message="no elements in sequence"});function Wh(e){return e instanceof Date&&!isNaN(e)}function L(e,n){return V((t,r)=>{let i=0;t.subscribe(B(r,o=>{r.next(e.call(n,o,i++))}))})}var{isArray:sC}=Array;function aC(e,n){return sC(n)?e(...n):e(n)}function Js(e){return L(n=>aC(e,n))}var{isArray:cC}=Array,{getPrototypeOf:lC,prototype:dC,keys:uC}=Object;function ea(e){if(e.length===1){let n=e[0];if(cC(n))return{args:n,keys:null};if(fC(n)){let t=uC(n);return{args:t.map(r=>n[r]),keys:t}}}return{args:e,keys:null}}function fC(e){return e&&typeof e=="object"&&lC(e)===dC}function ta(e,n){return e.reduce((t,r,i)=>(t[r]=n[i],t),{})}function nd(...e){let n=Ft(e),t=Vs(e),{args:r,keys:i}=ea(e);if(r.length===0)return de([],n);let o=new j(pC(r,n,i?s=>ta(i,s):Ue));return t?o.pipe(Js(t)):o}function pC(e,n,t=Ue){return r=>{qh(n,()=>{let{length:i}=e,o=new Array(i),s=i,a=i;for(let c=0;c<i;c++)qh(n,()=>{let l=de(e[c],n),d=!1;l.subscribe(B(r,u=>{o[c]=u,d||(d=!0,a--),a||r.next(t(o.slice()))},()=>{--s||r.complete()}))},r)},r)}}function qh(e,n,t){e?Ye(t,e,n):n()}function Zh(e,n,t,r,i,o,s,a){let c=[],l=0,d=0,u=!1,p=()=>{u&&!c.length&&!l&&n.complete()},h=I=>l<r?g(I):c.push(I),g=I=>{o&&n.next(I),l++;let D=!1;ne(t(I,d++)).subscribe(B(n,w=>{i?.(w),o?h(w):n.next(w)},()=>{D=!0},void 0,()=>{if(D)try{for(l--;c.length&&l<r;){let w=c.shift();s?Ye(n,s,()=>g(w)):g(w)}p()}catch(w){n.error(w)}}))};return e.subscribe(B(n,h,()=>{u=!0,p()})),()=>{a?.()}}function $e(e,n,t=1/0){return O(n)?$e((r,i)=>L((o,s)=>n(r,o,i,s))(ne(e(r,i))),t):(typeof n=="number"&&(t=n),V((r,i)=>Zh(r,i,e,t)))}function na(e=1/0){return $e(Ue,e)}function Qh(){return na(1)}function Ur(...e){return Qh()(de(e,Ft(e)))}function Wi(e){return new j(n=>{ne(e()).subscribe(n)})}function qi(...e){let n=Vs(e),{args:t,keys:r}=ea(e),i=new j(o=>{let{length:s}=t;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let u=!1;ne(t[d]).subscribe(B(o,p=>{u||(u=!0,l--),a[d]=p},()=>c--,void 0,()=>{(!c||!u)&&(l||o.next(r?ta(r,a):a),o.complete())}))}});return n?i.pipe(Js(n)):i}function Yh(e=0,n,t=Oh){let r=-1;return n!=null&&(js(n)?t=n:r=n),new j(i=>{let o=Wh(e)?+e-t.now():e;o<0&&(o=0);let s=0;return t.schedule(function(){i.closed||(i.next(s++),0<=r?this.schedule(void 0,r):i.complete())},o)})}function Zi(...e){let n=Ft(e),t=kh(e,1/0),r=e;return r.length?r.length===1?ne(r[0]):na(t)(de(r,n)):De}function Se(e,n){return V((t,r)=>{let i=0;t.subscribe(B(r,o=>e.call(n,o,i++)&&r.next(o)))})}function Pt(e){return V((n,t)=>{let r=null,i=!1,o;r=n.subscribe(B(t,void 0,void 0,s=>{o=ne(e(s,Pt(e)(n))),r?(r.unsubscribe(),r=null,o.subscribe(t)):i=!0})),i&&(r.unsubscribe(),r=null,o.subscribe(t))})}function $r(e,n){return O(n)?$e(e,n,1):$e(e,1)}function Kh(e){return V((n,t)=>{let r=!1;n.subscribe(B(t,i=>{r=!0,t.next(i)},()=>{r||t.next(e),t.complete()}))})}function mt(e){return e<=0?()=>De:V((n,t)=>{let r=0;n.subscribe(B(t,i=>{++r<=e&&(t.next(i),e<=r&&t.complete())}))})}function rd(e,n=Ue){return e=e??hC,V((t,r)=>{let i,o=!0;t.subscribe(B(r,s=>{let a=n(s);(o||!e(i,a))&&(o=!1,i=a,r.next(s))}))})}function hC(e,n){return e===n}function Xh(e=mC){return V((n,t)=>{let r=!1;n.subscribe(B(t,i=>{r=!0,t.next(i)},()=>r?t.complete():t.error(e())))})}function mC(){return new er}function tr(e){return V((n,t)=>{try{n.subscribe(t)}finally{t.add(e)}})}function Xt(e,n){let t=arguments.length>=2;return r=>r.pipe(e?Se((i,o)=>e(i,o,r)):Ue,mt(1),t?Kh(n):Xh(()=>new er))}function ra(e){return e<=0?()=>De:V((n,t)=>{let r=[];n.subscribe(B(t,i=>{r.push(i),e<r.length&&r.shift()},()=>{for(let i of r)t.next(i);t.complete()},void 0,()=>{r=null}))})}function id(){return V((e,n)=>{let t,r=!1;e.subscribe(B(n,i=>{let o=t;t=i,r&&n.next([o,i]),r=!0}))})}function od(e=1/0){let n;e&&typeof e=="object"?n=e:n={count:e};let{count:t=1/0,delay:r,resetOnSuccess:i=!1}=n;return t<=0?Ue:V((o,s)=>{let a=0,c,l=()=>{let d=!1;c=o.subscribe(B(s,u=>{i&&(a=0),s.next(u)},void 0,u=>{if(a++<t){let p=()=>{c?(c.unsubscribe(),c=null,l()):d=!0};if(r!=null){let h=typeof r=="number"?Yh(r):ne(r(u,a)),g=B(s,()=>{g.unsubscribe(),p()},()=>{s.complete()});h.subscribe(g)}else p()}else s.error(u)})),d&&(c.unsubscribe(),c=null,l())};l()})}function Qi(e={}){let{connector:n=()=>new H,resetOnError:t=!0,resetOnComplete:r=!0,resetOnRefCountZero:i=!0}=e;return o=>{let s,a,c,l=0,d=!1,u=!1,p=()=>{a?.unsubscribe(),a=void 0},h=()=>{p(),s=c=void 0,d=u=!1},g=()=>{let I=s;h(),I?.unsubscribe()};return V((I,D)=>{l++,!u&&!d&&p();let w=c=c??n();D.add(()=>{l--,l===0&&!u&&!d&&(a=sd(g,i))}),w.subscribe(D),!s&&l>0&&(s=new Kt({next:ce=>w.next(ce),error:ce=>{u=!0,p(),a=sd(h,t,ce),w.error(ce)},complete:()=>{d=!0,p(),a=sd(h,r),w.complete()}}),ne(I).subscribe(s))})(o)}}function sd(e,n,...t){if(n===!0){e();return}if(n===!1)return;let r=new Kt({next:()=>{r.unsubscribe(),e()}});return ne(n(...t)).subscribe(r)}function ad(e,n,t){let r,i=!1;return e&&typeof e=="object"?{bufferSize:r=1/0,windowTime:n=1/0,refCount:i=!1,scheduler:t}=e:r=e??1/0,Qi({connector:()=>new ks(r,n,t),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:i})}function cd(e){return Se((n,t)=>e<=t)}function Yi(...e){let n=Ft(e);return V((t,r)=>{(n?Ur(e,t,n):Ur(e,t)).subscribe(r)})}function Je(e,n){return V((t,r)=>{let i=null,o=0,s=!1,a=()=>s&&!i&&r.complete();t.subscribe(B(r,c=>{i?.unsubscribe();let l=0,d=o++;ne(e(c,d)).subscribe(i=B(r,u=>r.next(n?n(c,u,d,l++):u),()=>{i=null,a()}))},()=>{s=!0,a()}))})}function gt(e){return V((n,t)=>{ne(e).subscribe(B(t,()=>t.complete(),$i)),!t.closed&&n.subscribe(t)})}function Ne(e,n,t){let r=O(e)||n||t?{next:e,error:n,complete:t}:e;return r?V((i,o)=>{var s;(s=r.subscribe)===null||s===void 0||s.call(r);let a=!0;i.subscribe(B(o,c=>{var l;(l=r.next)===null||l===void 0||l.call(r,c),o.next(c)},()=>{var c;a=!1,(c=r.complete)===null||c===void 0||c.call(r),o.complete()},c=>{var l;a=!1,(l=r.error)===null||l===void 0||l.call(r,c),o.error(c)},()=>{var c,l;a&&((c=r.unsubscribe)===null||c===void 0||c.call(r)),(l=r.finalize)===null||l===void 0||l.call(r)}))}):Ue}var ld;function ia(){return ld}function Lt(e){let n=ld;return ld=e,n}var Jh=Symbol("NotFound");function zr(e){return e===Jh||e?.name==="\u0275NotFound"}function em(e){let n=A(null);try{return e()}finally{A(n)}}var ua="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",b=class extends Error{code;constructor(n,t){super(Cn(n,t)),this.code=n}};function gC(e){return`NG0${Math.abs(e)}`}function Cn(e,n){return`${gC(e)}${n?": "+n:""}`}var Wr=globalThis;function K(e){for(let n in e)if(e[n]===K)return n;throw Error("")}function om(e,n){for(let t in n)n.hasOwnProperty(t)&&!e.hasOwnProperty(t)&&(e[t]=n[t])}function ro(e){if(typeof e=="string")return e;if(Array.isArray(e))return`[${e.map(ro).join(", ")}]`;if(e==null)return""+e;let n=e.overriddenName||e.name;if(n)return`${n}`;let t=e.toString();if(t==null)return""+t;let r=t.indexOf(`
`);return r>=0?t.slice(0,r):t}function fa(e,n){return e?n?`${e} ${n}`:e:n||""}var vC=K({__forward_ref__:K});function En(e){return e.__forward_ref__=En,e}function Te(e){return Cd(e)?e():e}function Cd(e){return typeof e=="function"&&e.hasOwnProperty(vC)&&e.__forward_ref__===En}function y(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function G(e){return{providers:e.providers||[],imports:e.imports||[]}}function io(e){return yC(e,pa)}function Ed(e){return io(e)!==null}function yC(e,n){return e.hasOwnProperty(n)&&e[n]||null}function bC(e){let n=e?.[pa]??null;return n||null}function ud(e){return e&&e.hasOwnProperty(sa)?e[sa]:null}var pa=K({\u0275prov:K}),sa=K({\u0275inj:K}),m=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,t){this._desc=n,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=y({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function wd(e){return e&&!!e.\u0275providers}var Id=K({\u0275cmp:K}),xd=K({\u0275dir:K}),Md=K({\u0275pipe:K}),Sd=K({\u0275mod:K}),Xi=K({\u0275fac:K}),sr=K({__NG_ELEMENT_ID__:K}),tm=K({__NG_ENV_ID__:K});function Td(e){return ha(e,"@NgModule"),e[Sd]||null}function en(e){return ha(e,"@Component"),e[Id]||null}function Ad(e){return ha(e,"@Directive"),e[xd]||null}function sm(e){return ha(e,"@Pipe"),e[Md]||null}function ha(e,n){if(e==null)throw new b(-919,!1)}function ma(e){return typeof e=="string"?e:e==null?"":String(e)}var am=K({ngErrorCode:K}),_C=K({ngErrorMessage:K}),DC=K({ngTokenPath:K});function Rd(e,n){return cm("",-200,n)}function ga(e,n){throw new b(-201,!1)}function cm(e,n,t){let r=new b(n,e);return r[am]=n,r[_C]=e,t&&(r[DC]=t),r}function CC(e){return e[am]}var fd;function lm(){return fd}function ze(e){let n=fd;return fd=e,n}function Nd(e,n,t){let r=io(e);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(t&8)return null;if(n!==void 0)return n;ga(e,"")}var EC={},nr=EC,wC="__NG_DI_FLAG__",pd=class{injector;constructor(n){this.injector=n}retrieve(n,t){let r=rr(t)||0;try{return this.injector.get(n,r&8?null:nr,r)}catch(i){if(zr(i))return i;throw i}}};function IC(e,n=0){let t=ia();if(t===void 0)throw new b(-203,!1);if(t===null)return Nd(e,void 0,n);{let r=xC(n),i=t.retrieve(e,r);if(zr(i)){if(r.optional)return null;throw i}return i}}function E(e,n=0){return(lm()||IC)(Te(e),n)}function f(e,n){return E(e,rr(n))}function rr(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function xC(e){return{optional:!!(e&8),host:!!(e&1),self:!!(e&2),skipSelf:!!(e&4)}}function hd(e){let n=[];for(let t=0;t<e.length;t++){let r=Te(e[t]);if(Array.isArray(r)){if(r.length===0)throw new b(900,!1);let i,o=0;for(let s=0;s<r.length;s++){let a=r[s],c=MC(a);typeof c=="number"?c===-1?i=a.token:o|=c:i=a}n.push(E(i,o))}else n.push(E(r))}return n}function MC(e){return e[wC]}function bn(e,n){let t=e.hasOwnProperty(Xi);return t?e[Xi]:null}function dm(e,n,t){if(e.length!==n.length)return!1;for(let r=0;r<e.length;r++){let i=e[r],o=n[r];if(t&&(i=t(i),o=t(o)),o!==i)return!1}return!0}function um(e){return e.flat(Number.POSITIVE_INFINITY)}function va(e,n){e.forEach(t=>Array.isArray(t)?va(t,n):n(t))}function Od(e,n,t){n>=e.length?e.push(t):e.splice(n,0,t)}function oo(e,n){return n>=e.length-1?e.pop():e.splice(n,1)[0]}function fm(e,n){let t=[];for(let r=0;r<e;r++)t.push(n);return t}function pm(e,n,t,r){let i=e.length;if(i==n)e.push(t,r);else if(i===1)e.push(r,e[0]),e[0]=t;else{for(i--,e.push(e[i-1],e[i]);i>n;){let o=i-2;e[i]=e[o],i--}e[n]=t,e[n+1]=r}}function ya(e,n,t){let r=qr(e,n);return r>=0?e[r|1]=t:(r=~r,pm(e,r,n,t)),r}function ba(e,n){let t=qr(e,n);if(t>=0)return e[t|1]}function qr(e,n){return SC(e,n,1)}function SC(e,n,t){let r=0,i=e.length>>t;for(;i!==r;){let o=r+(i-r>>1),s=e[o<<t];if(n===s)return o<<t;s>n?i=o:r=o+1}return~(i<<t)}var wn={},Oe=[],ar=new m(""),kd=new m("",-1),Fd=new m(""),Ji=class{get(n,t=nr){if(t===nr){let i=cm("",-201);throw i.name="\u0275NotFound",i}return t}};function cr(e){return{\u0275providers:e}}function hm(...e){return{\u0275providers:Pd(!0,e),\u0275fromNgModule:!0}}function Pd(e,...n){let t=[],r=new Set,i,o=s=>{t.push(s)};return va(n,s=>{let a=s;aa(a,o,[],r)&&(i||=[],i.push(a))}),i!==void 0&&mm(i,o),t}function mm(e,n){for(let t=0;t<e.length;t++){let{ngModule:r,providers:i}=e[t];Ld(i,o=>{n(o,r)})}}function aa(e,n,t,r){if(e=Te(e),!e)return!1;let i=null,o=ud(e),s=!o&&en(e);if(!o&&!s){let c=e.ngModule;if(o=ud(c),o)i=c;else return!1}else{if(s&&!s.standalone)return!1;i=e}let a=r.has(i);if(s){if(a)return!1;if(r.add(i),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)aa(l,n,t,r)}}else if(o){if(o.imports!=null&&!a){r.add(i);let l;va(o.imports,d=>{aa(d,n,t,r)&&(l||=[],l.push(d))}),l!==void 0&&mm(l,n)}if(!a){let l=bn(i)||(()=>new i);n({provide:i,useFactory:l,deps:Oe},i),n({provide:Fd,useValue:i,multi:!0},i),n({provide:ar,useValue:()=>E(i),multi:!0},i)}let c=o.providers;if(c!=null&&!a){let l=e;Ld(c,d=>{n(d,l)})}}else return!1;return i!==e&&e.providers!==void 0}function Ld(e,n){for(let t of e)wd(t)&&(t=t.\u0275providers),Array.isArray(t)?Ld(t,n):n(t)}var TC=K({provide:String,useValue:K});function gm(e){return e!==null&&typeof e=="object"&&TC in e}function AC(e){return!!(e&&e.useExisting)}function RC(e){return!!(e&&e.useFactory)}function ir(e){return typeof e=="function"}function vm(e){return!!e.useClass}var so=new m(""),oa={},nm={},dd;function Zr(){return dd===void 0&&(dd=new Ji),dd}var ue=class{},or=class extends ue{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,t,r,i){super(),this.parent=t,this.source=r,this.scopes=i,gd(n,s=>this.processProvider(s)),this.records.set(kd,Gr(void 0,this)),i.has("environment")&&this.records.set(ue,Gr(void 0,this));let o=this.records.get(so);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Fd,Oe,{self:!0}))}retrieve(n,t){let r=rr(t)||0;try{return this.get(n,nr,r)}catch(i){if(zr(i))return i;throw i}}destroy(){Ki(this),this._destroyed=!0;let n=A(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of t)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),A(n)}}onDestroy(n){return Ki(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Ki(this);let t=Lt(this),r=ze(void 0),i;try{return n()}finally{Lt(t),ze(r)}}get(n,t=nr,r){if(Ki(this),n.hasOwnProperty(tm))return n[tm](this);let i=rr(r),o,s=Lt(this),a=ze(void 0);try{if(!(i&4)){let l=this.records.get(n);if(l===void 0){let d=PC(n)&&io(n);d&&this.injectableDefInScope(d)?l=Gr(md(n),oa):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,i)}let c=i&2?Zr():this.parent;return t=i&8&&t===nr?null:t,c.get(n,t)}catch(c){let l=CC(c);throw l===-200||l===-201?new b(l,null):c}finally{ze(a),Lt(s)}}resolveInjectorInitializers(){let n=A(null),t=Lt(this),r=ze(void 0),i;try{let o=this.get(ar,Oe,{self:!0});for(let s of o)s()}finally{Lt(t),ze(r),A(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Te(n);let t=ir(n)?n:Te(n&&n.provide),r=OC(n);if(!ir(n)&&n.multi===!0){let i=this.records.get(t);i||(i=Gr(void 0,oa,!0),i.factory=()=>hd(i.multi),this.records.set(t,i)),t=n,i.multi.push(n)}this.records.set(t,r)}hydrate(n,t,r){let i=A(null);try{if(t.value===nm)throw Rd("");return t.value===oa&&(t.value=nm,t.value=t.factory(void 0,r)),typeof t.value=="object"&&t.value&&FC(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{A(i)}}injectableDefInScope(n){if(!n.providedIn)return!1;let t=Te(n.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(n){let t=this._onDestroyHooks.indexOf(n);t!==-1&&this._onDestroyHooks.splice(t,1)}};function md(e){let n=io(e),t=n!==null?n.factory:bn(e);if(t!==null)return t;if(e instanceof m)throw new b(-204,!1);if(e instanceof Function)return NC(e);throw new b(-204,!1)}function NC(e){if(e.length>0)throw new b(-204,!1);let t=bC(e);return t!==null?()=>t.factory(e):()=>new e}function OC(e){if(gm(e))return Gr(void 0,e.useValue);{let n=jd(e);return Gr(n,oa)}}function jd(e,n,t){let r;if(ir(e)){let i=Te(e);return bn(i)||md(i)}else if(gm(e))r=()=>Te(e.useValue);else if(RC(e))r=()=>e.useFactory(...hd(e.deps||[]));else if(AC(e))r=(i,o)=>E(Te(e.useExisting),o!==void 0&&o&8?8:void 0);else{let i=Te(e&&(e.useClass||e.provide));if(kC(e))r=()=>new i(...hd(e.deps));else return bn(i)||md(i)}return r}function Ki(e){if(e.destroyed)throw new b(-205,!1)}function Gr(e,n,t=!1){return{factory:e,value:n,multi:t?[]:void 0}}function kC(e){return!!e.deps}function FC(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function PC(e){return typeof e=="function"||typeof e=="object"&&e.ngMetadataName==="InjectionToken"}function gd(e,n){for(let t of e)Array.isArray(t)?gd(t,n):t&&wd(t)?gd(t.\u0275providers,n):n(t)}function we(e,n){let t;e instanceof or?(Ki(e),t=e):t=new pd(e);let r,i=Lt(t),o=ze(void 0);try{return n()}finally{Lt(i),ze(o)}}function ym(){return lm()!==void 0||ia()!=null}var vt=0,x=1,T=2,Ee=3,ot=4,Ge=5,Qr=6,Yr=7,Ae=8,In=9,yt=10,ie=11,Kr=12,Vd=13,lr=14,Xe=15,xn=16,dr=17,Vt=18,Mn=19,Bd=20,Jt=21,_a=22,_n=23,et=24,ur=25,Xr=26,me=27,bm=1;var Sn=7,ao=8,fr=9,ke=10;function tn(e){return Array.isArray(e)&&typeof e[bm]=="object"}function bt(e){return Array.isArray(e)&&e[bm]===!0}function Hd(e){return(e.flags&4)!==0}function nn(e){return e.componentOffset>-1}function Jr(e){return(e.flags&1)===1}function Bt(e){return!!e.template}function ei(e){return(e[T]&512)!==0}function pr(e){return(e[T]&256)===256}var Ud="svg",_m="math";function st(e){for(;Array.isArray(e);)e=e[vt];return e}function $d(e,n){return st(n[e])}function _t(e,n){return st(n[e.index])}function Da(e,n){return e.data[n]}function zd(e,n){return e[n]}function Gd(e,n,t,r){t>=e.data.length&&(e.data[t]=null,e.blueprint[t]=null),n[t]=r}function at(e,n){let t=n[e];return tn(t)?t:t[vt]}function Dm(e){return(e[T]&4)===4}function Ca(e){return(e[T]&128)===128}function Cm(e){return bt(e[Ee])}function Ht(e,n){return n==null?null:e[n]}function Wd(e){e[dr]=0}function qd(e){e[T]&1024||(e[T]|=1024,Ca(e)&&hr(e))}function Em(e,n){for(;e>0;)n=n[lr],e--;return n}function co(e){return!!(e[T]&9216||e[et]?.dirty)}function Ea(e){e[yt].changeDetectionScheduler?.notify(8),e[T]&64&&(e[T]|=1024),co(e)&&hr(e)}function hr(e){e[yt].changeDetectionScheduler?.notify(0);let n=Dn(e);for(;n!==null&&!(n[T]&8192||(n[T]|=8192,!Ca(n)));)n=Dn(n)}function Zd(e,n){if(pr(e))throw new b(911,!1);e[Jt]===null&&(e[Jt]=[]),e[Jt].push(n)}function wm(e,n){if(e[Jt]===null)return;let t=e[Jt].indexOf(n);t!==-1&&e[Jt].splice(t,1)}function Dn(e){let n=e[Ee];return bt(n)?n[Ee]:n}function Qd(e){return e[Yr]??=[]}function Yd(e){return e.cleanup??=[]}function Im(e,n,t,r){let i=Qd(n);i.push(t),e.firstCreatePass&&Yd(e).push(r,i.length-1)}var k={lFrame:Pm(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var vd=!1;function xm(){return k.lFrame.elementDepthCount}function Mm(){k.lFrame.elementDepthCount++}function Kd(){k.lFrame.elementDepthCount--}function wa(){return k.bindingsEnabled}function Xd(){return k.skipHydrationRootTNode!==null}function Jd(e){return k.skipHydrationRootTNode===e}function eu(){k.skipHydrationRootTNode=null}function R(){return k.lFrame.lView}function fe(){return k.lFrame.tView}function Ia(e){return k.lFrame.contextLView=e,e[Ae]}function xa(e){return k.lFrame.contextLView=null,e}function ye(){let e=tu();for(;e!==null&&e.type===64;)e=e.parent;return e}function tu(){return k.lFrame.currentTNode}function Sm(){let e=k.lFrame,n=e.currentTNode;return e.isParent?n:n.parent}function ti(e,n){let t=k.lFrame;t.currentTNode=e,t.isParent=n}function nu(){return k.lFrame.isParent}function ru(){k.lFrame.isParent=!1}function Tm(){return k.lFrame.contextLView}function iu(){return vd}function eo(e){let n=vd;return vd=e,n}function ou(){let e=k.lFrame,n=e.bindingRootIndex;return n===-1&&(n=e.bindingRootIndex=e.tView.bindingStartIndex),n}function Am(e){return k.lFrame.bindingIndex=e}function mr(){return k.lFrame.bindingIndex++}function su(e){let n=k.lFrame,t=n.bindingIndex;return n.bindingIndex=n.bindingIndex+e,t}function Rm(){return k.lFrame.inI18n}function Nm(e,n){let t=k.lFrame;t.bindingIndex=t.bindingRootIndex=e,Ma(n)}function Om(){return k.lFrame.currentDirectiveIndex}function Ma(e){k.lFrame.currentDirectiveIndex=e}function km(e){let n=k.lFrame.currentDirectiveIndex;return n===-1?null:e[n]}function Sa(){return k.lFrame.currentQueryIndex}function lo(e){k.lFrame.currentQueryIndex=e}function LC(e){let n=e[x];return n.type===2?n.declTNode:n.type===1?e[Ge]:null}function au(e,n,t){if(t&4){let i=n,o=e;for(;i=i.parent,i===null&&!(t&1);)if(i=LC(o),i===null||(o=o[lr],i.type&10))break;if(i===null)return!1;n=i,e=o}let r=k.lFrame=Fm();return r.currentTNode=n,r.lView=e,!0}function Ta(e){let n=Fm(),t=e[x];k.lFrame=n,n.currentTNode=t.firstChild,n.lView=e,n.tView=t,n.contextLView=e,n.bindingIndex=t.bindingStartIndex,n.inI18n=!1}function Fm(){let e=k.lFrame,n=e===null?null:e.child;return n===null?Pm(e):n}function Pm(e){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=n),n}function Lm(){let e=k.lFrame;return k.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var cu=Lm;function Aa(){let e=Lm();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function jm(e){return(k.lFrame.contextLView=Em(e,k.lFrame.contextLView))[Ae]}function Tn(){return k.lFrame.selectedIndex}function An(e){k.lFrame.selectedIndex=e}function uo(){let e=k.lFrame;return Da(e.tView,e.selectedIndex)}function Rn(){k.lFrame.currentNamespace=Ud}function ni(){jC()}function jC(){k.lFrame.currentNamespace=null}function Vm(){return k.lFrame.currentNamespace}var Bm=!0;function Ra(){return Bm}function fo(e){Bm=e}function yd(e,n=null,t=null,r){let i=lu(e,n,t,r);return i.resolveInjectorInitializers(),i}function lu(e,n=null,t=null,r,i=new Set){let o=[t||Oe,hm(e)],s;return new or(o,n||Zr(),s||null,i)}var he=class e{static THROW_IF_NOT_FOUND=nr;static NULL=new Ji;static create(n,t){if(Array.isArray(n))return yd({name:""},t,n,"");{let r=n.name??"";return yd({name:r},n.parent,n.providers,r)}}static \u0275prov=y({token:e,providedIn:"any",factory:()=>E(kd)});static __NG_ELEMENT_ID__=-1},U=new m(""),Dt=(()=>{class e{static __NG_ELEMENT_ID__=VC;static __NG_ENV_ID__=t=>t}return e})(),ca=class extends Dt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return pr(this._lView)}onDestroy(n){let t=this._lView;return Zd(t,n),()=>wm(t,n)}};function VC(){return new ca(R())}var Hm=!1,Um=new m(""),rn=(()=>{class e{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new _e(!1);debugTaskTracker=f(Um,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new j(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=y({token:e,providedIn:"root",factory:()=>new e})}return e})(),bd=class extends H{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,ym()&&(this.destroyRef=f(Dt,{optional:!0})??void 0,this.pendingTasks=f(rn,{optional:!0})??void 0)}emit(n){let t=A(null);try{super.next(n)}finally{A(t)}}subscribe(n,t,r){let i=n,o=t||(()=>null),s=r;if(n&&typeof n=="object"){let c=n;i=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),i&&(i=this.wrapInTimeout(i)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:i,error:o,complete:s});return n instanceof pe&&n.add(a),a}wrapInTimeout(n){return t=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{n(t)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},re=bd;function la(...e){}function du(e){let n,t;function r(){e=la;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{e(),r()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{e(),r()})),()=>r()}function $m(e){return queueMicrotask(()=>e()),()=>{e=la}}var uu="isAngularZone",to=uu+"_ID",BC=0,$=class e{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new re(!1);onMicrotaskEmpty=new re(!1);onStable=new re(!1);onError=new re(!1);constructor(n){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:i=!1,scheduleInRootZone:o=Hm}=n;if(typeof Zone>"u")throw new b(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!i&&r,s.shouldCoalesceRunChangeDetection=i,s.callbackScheduled=!1,s.scheduleInRootZone=o,$C(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(uu)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new b(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new b(909,!1)}run(n,t,r){return this._inner.run(n,t,r)}runTask(n,t,r,i){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+i,n,HC,la,la);try{return o.runTask(s,t,r)}finally{o.cancelTask(s)}}runGuarded(n,t,r){return this._inner.runGuarded(n,t,r)}runOutsideAngular(n){return this._outer.run(n)}},HC={};function fu(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function UC(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function n(){du(()=>{e.callbackScheduled=!1,_d(e),e.isCheckStableRunning=!0,fu(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{n()}):e._outer.run(()=>{n()}),_d(e)}function $C(e){let n=()=>{UC(e)},t=BC++;e._inner=e._inner.fork({name:"angular",properties:{[uu]:!0,[to]:t,[to+t]:!0},onInvokeTask:(r,i,o,s,a,c)=>{if(zC(c))return r.invokeTask(o,s,a,c);try{return rm(e),r.invokeTask(o,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&n(),im(e)}},onInvoke:(r,i,o,s,a,c,l)=>{try{return rm(e),r.invoke(o,s,a,c,l)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!GC(c)&&n(),im(e)}},onHasTask:(r,i,o,s)=>{r.hasTask(o,s),i===o&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,_d(e),fu(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,i,o,s)=>(r.handleError(o,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function _d(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function rm(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function im(e){e._nesting--,fu(e)}var no=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new re;onMicrotaskEmpty=new re;onStable=new re;onError=new re;run(n,t,r){return n.apply(t,r)}runGuarded(n,t,r){return n.apply(t,r)}runOutsideAngular(n){return n()}runTask(n,t,r,i){return n.apply(t,r)}};function zC(e){return zm(e,"__ignore_ng_zone__")}function GC(e){return zm(e,"__scheduler_tick__")}function zm(e,n){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[n]===!0}var Ke=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Ct=new m("",{factory:()=>{let e=f($),n=f(ue),t;return r=>{e.runOutsideAngular(()=>{n.destroyed&&!t?setTimeout(()=>{throw r}):(t??=n.get(Ke),t.handleError(r))})}}}),Gm={provide:ar,useValue:()=>{let e=f(Ke,{optional:!0})},multi:!0};function We(e,n){let[t,r,i]=$l(e,n?.equal),o=t,s=o[be];return o.set=r,o.update=i,o.asReadonly=Wm.bind(o),o}function Wm(){let e=this[be];if(e.readonlyFn===void 0){let n=()=>this();n[be]=e,e.readonlyFn=n}return e.readonlyFn}var po=(()=>{class e{view;node;constructor(t,r){this.view=t,this.node=r}static __NG_ELEMENT_ID__=WC}return e})();function WC(){return new po(R(),ye())}var jt=class{},ho=new m("",{factory:()=>!0});var pu=new m(""),Na=(()=>{class e{internalPendingTasks=f(rn);scheduler=f(jt);errorHandler=f(Ct);add(){let t=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(t)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(t))}}run(t){let r=this.add();t().catch(this.errorHandler).finally(r)}static \u0275prov=y({token:e,providedIn:"root",factory:()=>new e})}return e})(),Oa=(()=>{class e{static \u0275prov=y({token:e,providedIn:"root",factory:()=>new Dd})}return e})(),Dd=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let t=n.zone,r=this.queues.get(t);r.has(n)&&(r.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let t=n.zone;this.queues.has(t)||this.queues.set(t,new Set);let r=this.queues.get(t);r.has(n)||r.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[t,r]of this.queues)t===null?n||=this.flushQueue(r):n||=t.run(()=>this.flushQueue(r));n||(this.dirtyEffectCount=0)}}flushQueue(n){let t=!1;for(let r of n)r.dirty&&(this.dirtyEffectCount--,t=!0,r.run());return t}},da=class{[be];constructor(n){this[be]=n}destroy(){this[be].destroy()}};function hu(e,n){let t=n?.injector??f(he),r=n?.manualCleanup!==!0?t.get(Dt):null,i,o=t.get(po,null,{optional:!0}),s=t.get(jt);return o!==null?(i=QC(o.view,s,e),r instanceof ca&&r._lView===o.view&&(r=null)):i=YC(e,t.get(Oa),s),i.injector=t,r!==null&&(i.onDestroyFns=[r.onDestroy(()=>i.destroy())]),new da(i)}var qm=P(v({},Gl),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let e=eo(!1);try{Wl(this)}finally{eo(e)}},cleanup(){if(!this.cleanupFns?.length)return;let e=A(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],A(e)}}}),qC=P(v({},qm),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(yn(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.scheduler.remove(this)}}),ZC=P(v({},qm),{consumerMarkedDirty(){this.view[T]|=8192,hr(this.view),this.notifier.notify(13)},destroy(){if(yn(this),this.onDestroyFns!==null)for(let e of this.onDestroyFns)e();this.cleanup(),this.view[_n]?.delete(this)}});function QC(e,n,t){let r=Object.create(ZC);return r.view=e,r.zone=typeof Zone<"u"?Zone.current:null,r.notifier=n,r.fn=Zm(r,t),e[_n]??=new Set,e[_n].add(r),r.consumerMarkedDirty(r),r}function YC(e,n,t){let r=Object.create(qC);return r.fn=Zm(r,e),r.scheduler=n,r.notifier=t,r.zone=typeof Zone<"u"?Zone.current:null,r.scheduler.add(r),r.notifier.notify(12),r}function Zm(e,n){return()=>{n(t=>(e.cleanupFns??=[]).push(t))}}function Eo(e){return{toString:e}.toString()}function nE(e){return typeof e=="function"}function xg(e,n,t,r){n!==null?n.applyValueToInputSignal(n,r):e[t]=r}var Ua=class{previousValue;currentValue;firstChange;constructor(n,t,r){this.previousValue=n,this.currentValue=t,this.firstChange=r}isFirstChange(){return this.firstChange}},sn=(()=>{let e=()=>Mg;return e.ngInherit=!0,e})();function Mg(e){return e.type.prototype.ngOnChanges&&(e.setInput=iE),rE}function rE(){let e=Tg(this),n=e?.current;if(n){let t=e.previous;if(t===wn)e.previous=n;else for(let r in n)t[r]=n[r];e.current=null,this.ngOnChanges(n)}}function iE(e,n,t,r,i){let o=this.declaredInputs[r],s=Tg(e)||oE(e,{previous:wn,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new Ua(l&&l.currentValue,t,c===wn),xg(e,n,i,t)}var Sg="__ngSimpleChanges__";function Tg(e){return e[Sg]||null}function oE(e,n){return e[Sg]=n}var Qm=[];var X=function(e,n=null,t){for(let r=0;r<Qm.length;r++){let i=Qm[r];i(e,n,t)}},z=(function(e){return e[e.TemplateCreateStart=0]="TemplateCreateStart",e[e.TemplateCreateEnd=1]="TemplateCreateEnd",e[e.TemplateUpdateStart=2]="TemplateUpdateStart",e[e.TemplateUpdateEnd=3]="TemplateUpdateEnd",e[e.LifecycleHookStart=4]="LifecycleHookStart",e[e.LifecycleHookEnd=5]="LifecycleHookEnd",e[e.OutputStart=6]="OutputStart",e[e.OutputEnd=7]="OutputEnd",e[e.BootstrapApplicationStart=8]="BootstrapApplicationStart",e[e.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",e[e.BootstrapComponentStart=10]="BootstrapComponentStart",e[e.BootstrapComponentEnd=11]="BootstrapComponentEnd",e[e.ChangeDetectionStart=12]="ChangeDetectionStart",e[e.ChangeDetectionEnd=13]="ChangeDetectionEnd",e[e.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",e[e.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",e[e.AfterRenderHooksStart=16]="AfterRenderHooksStart",e[e.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",e[e.ComponentStart=18]="ComponentStart",e[e.ComponentEnd=19]="ComponentEnd",e[e.DeferBlockStateStart=20]="DeferBlockStateStart",e[e.DeferBlockStateEnd=21]="DeferBlockStateEnd",e[e.DynamicComponentStart=22]="DynamicComponentStart",e[e.DynamicComponentEnd=23]="DynamicComponentEnd",e[e.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",e[e.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",e})(z||{});function sE(e,n,t){let{ngOnChanges:r,ngOnInit:i,ngDoCheck:o}=n.type.prototype;if(r){let s=Mg(n);(t.preOrderHooks??=[]).push(e,s),(t.preOrderCheckHooks??=[]).push(e,s)}i&&(t.preOrderHooks??=[]).push(0-e,i),o&&((t.preOrderHooks??=[]).push(e,o),(t.preOrderCheckHooks??=[]).push(e,o))}function Ag(e,n){for(let t=n.directiveStart,r=n.directiveEnd;t<r;t++){let o=e.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(e.contentHooks??=[]).push(-t,s),a&&((e.contentHooks??=[]).push(t,a),(e.contentCheckHooks??=[]).push(t,a)),c&&(e.viewHooks??=[]).push(-t,c),l&&((e.viewHooks??=[]).push(t,l),(e.viewCheckHooks??=[]).push(t,l)),d!=null&&(e.destroyHooks??=[]).push(t,d)}}function La(e,n,t){Rg(e,n,3,t)}function ja(e,n,t,r){(e[T]&3)===t&&Rg(e,n,t,r)}function mu(e,n){let t=e[T];(t&3)===n&&(t&=16383,t+=1,e[T]=t)}function Rg(e,n,t,r){let i=r!==void 0?e[dr]&65535:0,o=r??-1,s=n.length-1,a=0;for(let c=i;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],r!=null&&a>=r)break}else n[c]<0&&(e[dr]+=65536),(a<o||o==-1)&&(aE(e,t,n,c),e[dr]=(e[dr]&4294901760)+c+2),c++}function Ym(e,n){X(z.LifecycleHookStart,e,n);let t=A(null);try{n.call(e)}finally{A(t),X(z.LifecycleHookEnd,e,n)}}function aE(e,n,t,r){let i=t[r]<0,o=t[r+1],s=i?-t[r]:t[r],a=e[s];i?e[T]>>14<e[dr]>>16&&(e[T]&3)===n&&(e[T]+=16384,Ym(a,o)):Ym(a,o)}var ii=-1,vr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,t,r,i){this.factory=n,this.name=i,this.canSeeViewProviders=t,this.injectImpl=r}};function cE(e){return(e.flags&8)!==0}function lE(e){return(e.flags&16)!==0}function dE(e,n,t){let r=0;for(;r<t.length;){let i=t[r];if(typeof i=="number"){if(i!==0)break;r++;let o=t[r++],s=t[r++],a=t[r++];e.setAttribute(n,s,a,o)}else{let o=i,s=t[++r];uE(o)?e.setProperty(n,o,s):e.setAttribute(n,o,s),r++}}return r}function Ng(e){return e===3||e===4||e===6}function uE(e){return e.charCodeAt(0)===64}function oi(e,n){if(!(n===null||n.length===0))if(e===null||e.length===0)e=n.slice();else{let t=-1;for(let r=0;r<n.length;r++){let i=n[r];typeof i=="number"?t=i:t===0||(t===-1||t===2?Km(e,t,i,null,n[++r]):Km(e,t,i,null,null))}}return e}function Km(e,n,t,r,i){let o=0,s=e.length;if(n===-1)s=-1;else for(;o<e.length;){let a=e[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<e.length;){let a=e[o];if(typeof a=="number")break;if(a===t){i!==null&&(e[o+1]=i);return}o++,i!==null&&o++}s!==-1&&(e.splice(s,0,n),o=s+1),e.splice(o++,0,t),i!==null&&e.splice(o++,0,i)}function Og(e){return e!==ii}function $a(e){return e&32767}function fE(e){return e>>16}function za(e,n){let t=fE(e),r=n;for(;t>0;)r=r[lr],t--;return r}var wu=!0;function Ga(e){let n=wu;return wu=e,n}var pE=256,kg=pE-1,Fg=5,hE=0,Ut={};function mE(e,n,t){let r;typeof t=="string"?r=t.charCodeAt(0)||0:t.hasOwnProperty(sr)&&(r=t[sr]),r==null&&(r=t[sr]=hE++);let i=r&kg,o=1<<i;n.data[e+(i>>Fg)]|=o}function Wa(e,n){let t=Pg(e,n);if(t!==-1)return t;let r=n[x];r.firstCreatePass&&(e.injectorIndex=n.length,gu(r.data,e),gu(n,null),gu(r.blueprint,null));let i=tf(e,n),o=e.injectorIndex;if(Og(i)){let s=$a(i),a=za(i,n),c=a[x].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=i,o}function gu(e,n){e.push(0,0,0,0,0,0,0,0,n)}function Pg(e,n){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||n[e.injectorIndex+8]===null?-1:e.injectorIndex}function tf(e,n){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let t=0,r=null,i=n;for(;i!==null;){if(r=Hg(i),r===null)return ii;if(t++,i=i[lr],r.injectorIndex!==-1)return r.injectorIndex|t<<16}return ii}function Iu(e,n,t){mE(e,n,t)}function gE(e,n){if(n==="class")return e.classes;if(n==="style")return e.styles;let t=e.attrs;if(t){let r=t.length,i=0;for(;i<r;){let o=t[i];if(Ng(o))break;if(o===0)i=i+2;else if(typeof o=="number")for(i++;i<r&&typeof t[i]=="string";)i++;else{if(o===n)return t[i+1];i=i+2}}}return null}function Lg(e,n,t){if(t&8||e!==void 0)return e;ga(n,"NodeInjector")}function jg(e,n,t,r){if(t&8&&r===void 0&&(r=null),(t&3)===0){let i=e[In],o=ze(void 0);try{return i?i.get(n,r,t&8):Nd(n,r,t&8)}finally{ze(o)}}return Lg(r,n,t)}function Vg(e,n,t,r=0,i){if(e!==null){if(n[T]&2048&&!(r&2)){let s=_E(e,n,t,r,Ut);if(s!==Ut)return s}let o=Bg(e,n,t,r,Ut);if(o!==Ut)return o}return jg(n,t,r,i)}function Bg(e,n,t,r,i){let o=yE(t);if(typeof o=="function"){if(!au(n,e,r))return r&1?Lg(i,t,r):jg(n,t,r,i);try{let s;if(s=o(r),s==null&&!(r&8))ga(t);else return s}finally{cu()}}else if(typeof o=="number"){let s=null,a=Pg(e,n),c=ii,l=r&1?n[Xe][Ge]:null;for((a===-1||r&4)&&(c=a===-1?tf(e,n):n[a+8],c===ii||!Jm(r,!1)?a=-1:(s=n[x],a=$a(c),n=za(c,n)));a!==-1;){let d=n[x];if(Xm(o,a,d.data)){let u=vE(a,n,t,s,r,l);if(u!==Ut)return u}c=n[a+8],c!==ii&&Jm(r,n[x].data[a+8]===l)&&Xm(o,a,n)?(s=d,a=$a(c),n=za(c,n)):a=-1}}return i}function vE(e,n,t,r,i,o){let s=n[x],a=s.data[e+8],c=r==null?nn(a)&&wu:r!=s&&(a.type&3)!==0,l=i&1&&o===a,d=Va(a,s,t,c,l);return d!==null?yo(n,s,d,a,i):Ut}function Va(e,n,t,r,i){let o=e.providerIndexes,s=n.data,a=o&1048575,c=e.directiveStart,l=e.directiveEnd,d=o>>20,u=r?a:a+d,p=i?a+d:l;for(let h=u;h<p;h++){let g=s[h];if(h<c&&t===g||h>=c&&g.type===t)return h}if(i){let h=s[c];if(h&&Bt(h)&&h.type===t)return c}return null}function yo(e,n,t,r,i){let o=e[t],s=n.data;if(o instanceof vr){let a=o;if(a.resolving)throw Rd("");let c=Ga(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],d,u=a.injectImpl?ze(a.injectImpl):null,p=au(e,r,0);try{o=e[t]=a.factory(void 0,i,s,e,r),n.firstCreatePass&&t>=r.directiveStart&&sE(t,s[t],n)}finally{u!==null&&ze(u),Ga(c),a.resolving=!1,cu()}}return o}function yE(e){if(typeof e=="string")return e.charCodeAt(0)||0;let n=e.hasOwnProperty(sr)?e[sr]:void 0;return typeof n=="number"?n>=0?n&kg:bE:n}function Xm(e,n,t){let r=1<<e;return!!(t[n+(e>>Fg)]&r)}function Jm(e,n){return!(e&2)&&!(e&1&&n)}var gr=class{_tNode;_lView;constructor(n,t){this._tNode=n,this._lView=t}get(n,t,r){return Vg(this._tNode,this._lView,n,rr(r),t)}};function bE(){return new gr(ye(),R())}function an(e){return Eo(()=>{let n=e.prototype.constructor,t=n[Xi]||xu(n),r=Object.prototype,i=Object.getPrototypeOf(e.prototype).constructor;for(;i&&i!==r;){let o=i[Xi]||xu(i);if(o&&o!==t)return o;i=Object.getPrototypeOf(i)}return o=>new o})}function xu(e){return Cd(e)?()=>{let n=xu(Te(e));return n&&n()}:bn(e)}function _E(e,n,t,r,i){let o=e,s=n;for(;o!==null&&s!==null&&s[T]&2048&&!ei(s);){let a=Bg(o,s,t,r|2,Ut);if(a!==Ut)return a;let c=o.parent;if(!c){let l=s[Bd];if(l){let d=l.get(t,Ut,r&-5);if(d!==Ut)return d}c=Hg(s),s=s[lr]}o=c}return i}function Hg(e){let n=e[x],t=n.type;return t===2?n.declTNode:t===1?e[Ge]:null}function nf(e){return gE(ye(),e)}function DE(){return li(ye(),R())}function li(e,n){return new J(_t(e,n))}var J=(()=>{class e{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=DE}return e})();function Ug(e){return e instanceof J?e.nativeElement:e}function CE(){return this._results[Symbol.iterator]()}var bo=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new H}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,t){return this._results.reduce(n,t)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,t){this.dirty=!1;let r=um(n);(this._changesDetected=!dm(this._results,r,t))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=CE};function $g(e){return(e.flags&128)===128}var rf=(function(e){return e[e.OnPush=0]="OnPush",e[e.Eager=1]="Eager",e[e.Default=1]="Default",e})(rf||{}),zg=new Map,EE=0;function wE(){return EE++}function IE(e){zg.set(e[Mn],e)}function Mu(e){zg.delete(e[Mn])}var eg="__ngContext__";function si(e,n){tn(n)?(e[eg]=n[Mn],IE(n)):e[eg]=n}function Gg(e){return qg(e[Kr])}function Wg(e){return qg(e[ot])}function qg(e){for(;e!==null&&!bt(e);)e=e[ot];return e}var xE;function of(e){xE=e}var di=new m("",{factory:()=>ME}),ME="ng";var oc=new m(""),Cr=new m("",{providedIn:"platform",factory:()=>"unknown"}),sf=new m(""),Er=new m("",{factory:()=>f(U).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Zg=!1,Qg=new m("",{factory:()=>Zg});var SE=(e,n,t,r)=>{};function TE(e,n,t,r){SE(e,n,t,r)}function sc(e){return(e.flags&32)===32}var AE=()=>null;function Yg(e,n,t=!1){return AE(e,n,t)}function Kg(e,n){let t=e.contentQueries;if(t!==null){let r=A(null);try{for(let i=0;i<t.length;i+=2){let o=t[i],s=t[i+1];if(s!==-1){let a=e.data[s];lo(o),a.contentQueries(2,n[s],s)}}}finally{A(r)}}}function Su(e,n,t){lo(0);let r=A(null);try{n(e,t)}finally{A(r)}}function af(e,n,t){if(Hd(n)){let r=A(null);try{let i=n.directiveStart,o=n.directiveEnd;for(let s=i;s<o;s++){let a=e.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{A(r)}}}var It=(function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e[e.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",e})(It||{});var ka;function RE(){if(ka===void 0&&(ka=null,Wr.trustedTypes))try{ka=Wr.trustedTypes.createPolicy("angular",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return ka}function ac(e){return RE()?.createHTML(e)||e}var on=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${ua})`}},Tu=class extends on{getTypeName(){return"HTML"}},Au=class extends on{getTypeName(){return"Style"}},Ru=class extends on{getTypeName(){return"Script"}},Nu=class extends on{getTypeName(){return"URL"}},Ou=class extends on{getTypeName(){return"ResourceURL"}};function zt(e){return e instanceof on?e.changingThisBreaksApplicationSecurity:e}function On(e,n){let t=Xg(e);if(t!=null&&t!==n){if(t==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${t} (see ${ua})`)}return t===n}function Xg(e){return e instanceof on&&e.getTypeName()||null}function cf(e){return new Tu(e)}function lf(e){return new Au(e)}function df(e){return new Ru(e)}function uf(e){return new Nu(e)}function ff(e){return new Ou(e)}function NE(e){let n=new Fu(e);return OE()?new ku(n):n}var ku=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let t=new window.DOMParser().parseFromString(ac(n),"text/html").body;return t===null?this.inertDocumentHelper.getInertBodyElement(n):(t.firstChild?.remove(),t)}catch{return null}}},Fu=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let t=this.inertDocument.createElement("template");return t.innerHTML=ac(n),t}};function OE(){try{return!!new window.DOMParser().parseFromString(ac(""),"text/html")}catch{return!1}}var kE=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function wo(e){return e=String(e),e.match(kE)?e:"unsafe:"+e}function cn(e){let n={};for(let t of e.split(","))n[t]=!0;return n}function Io(...e){let n={};for(let t of e)for(let r in t)t.hasOwnProperty(r)&&(n[r]=!0);return n}var Jg=cn("area,br,col,hr,img,wbr"),ev=cn("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),tv=cn("rp,rt"),FE=Io(tv,ev),PE=Io(ev,cn("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),LE=Io(tv,cn("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),tg=Io(Jg,PE,LE,FE),nv=cn("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),jE=cn("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),VE=cn("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),BE=Io(nv,jE,VE),HE=cn("script,style,template");var Pu=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let t=n.firstChild,r=!0,i=[];for(;t;){if(t.nodeType===Node.ELEMENT_NODE?r=this.startElement(t):t.nodeType===Node.TEXT_NODE?this.chars(t.nodeValue):this.sanitizedSomething=!0,r&&t.firstChild){i.push(t),t=zE(t);continue}for(;t;){t.nodeType===Node.ELEMENT_NODE&&this.endElement(t);let o=$E(t);if(o){t=o;break}t=i.pop()}}return this.buf.join("")}startElement(n){let t=ng(n).toLowerCase();if(!tg.hasOwnProperty(t))return this.sanitizedSomething=!0,!HE.hasOwnProperty(t);this.buf.push("<"),this.buf.push(t);let r=n.attributes;for(let i=0;i<r.length;i++){let o=r.item(i),s=o.name,a=s.toLowerCase();if(!BE.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;nv[a]&&(c=wo(c)),this.buf.push(" ",s,'="',rg(c),'"')}return this.buf.push(">"),!0}endElement(n){let t=ng(n).toLowerCase();tg.hasOwnProperty(t)&&!Jg.hasOwnProperty(t)&&(this.buf.push("</"),this.buf.push(t),this.buf.push(">"))}chars(n){this.buf.push(rg(n))}};function UE(e,n){return(e.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function $E(e){let n=e.nextSibling;if(n&&e!==n.previousSibling)throw rv(n);return n}function zE(e){let n=e.firstChild;if(n&&UE(e,n))throw rv(n);return n}function ng(e){let n=e.nodeName;return typeof n=="string"?n:"FORM"}function rv(e){return new Error(`Failed to sanitize html because the element is clobbered: ${e.outerHTML}`)}var GE=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,WE=/([^\#-~ |!])/g;function rg(e){return e.replace(/&/g,"&amp;").replace(GE,function(n){let t=n.charCodeAt(0),r=n.charCodeAt(1);return"&#"+((t-55296)*1024+(r-56320)+65536)+";"}).replace(WE,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Fa;function pf(e,n){let t=null;try{Fa=Fa||NE(e);let r=n?String(n):"";t=Fa.getInertBodyElement(r);let i=5,o=r;do{if(i===0)throw new Error("Failed to sanitize html because the input is unstable");i--,r=o,o=t.innerHTML,t=Fa.getInertBodyElement(r)}while(r!==o);let a=new Pu().sanitizeChildren(ig(t)||t);return ac(a)}finally{if(t){let r=ig(t)||t;for(;r.firstChild;)r.firstChild.remove()}}}function ig(e){return"content"in e&&qE(e)?e.content:null}function qE(e){return e.nodeType===Node.ELEMENT_NODE&&e.nodeName==="TEMPLATE"}var ZE=/^>|^->|<!--|-->|--!>|<!-$/g,QE=/(<|>)/g,YE="\u200B$1\u200B";function KE(e){return e.replace(ZE,n=>n.replace(QE,YE))}function XE(e,n){return e.createText(n)}function JE(e,n,t){e.setValue(n,t)}function ew(e,n){return e.createComment(KE(n))}function iv(e,n,t){return e.createElement(n,t)}function qa(e,n,t,r,i){e.insertBefore(n,t,r,i)}function ov(e,n,t){e.appendChild(n,t)}function og(e,n,t,r,i){r!==null?qa(e,n,t,r,i):ov(e,n,t)}function tw(e,n,t,r){e.removeChild(null,n,t,r)}function nw(e,n,t){e.setAttribute(n,"style",t)}function rw(e,n,t){t===""?e.removeAttribute(n,"class"):e.setAttribute(n,"class",t)}function sv(e,n,t){let{mergedAttrs:r,classes:i,styles:o}=t;r!==null&&dE(e,n,r),i!==null&&rw(e,n,i),o!==null&&nw(e,n,o)}var Fe=(function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e})(Fe||{});function hf(e){let n=iw();return n?n.sanitize(Fe.URL,e)||"":On(e,"URL")?zt(e):wo(ma(e))}function iw(){let e=R();return e&&e[yt].sanitizer}function av(e){return e instanceof Function?e():e}function ow(e,n,t){let r=e.length;for(;;){let i=e.indexOf(n,t);if(i===-1)return i;if(i===0||e.charCodeAt(i-1)<=32){let o=n.length;if(i+o===r||e.charCodeAt(i+o)<=32)return i}t=i+1}}var cv="ng-template";function sw(e,n,t,r){let i=0;if(r){for(;i<n.length&&typeof n[i]=="string";i+=2)if(n[i]==="class"&&ow(n[i+1].toLowerCase(),t,0)!==-1)return!0}else if(mf(e))return!1;if(i=n.indexOf(1,i),i>-1){let o;for(;++i<n.length&&typeof(o=n[i])=="string";)if(o.toLowerCase()===t)return!0}return!1}function mf(e){return e.type===4&&e.value!==cv}function aw(e,n,t){let r=e.type===4&&!t?cv:e.value;return n===r}function cw(e,n,t){let r=4,i=e.attrs,o=i!==null?uw(i):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!Et(r)&&!Et(c))return!1;if(s&&Et(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!aw(e,c,t)||c===""&&n.length===1){if(Et(r))return!1;s=!0}}else if(r&8){if(i===null||!sw(e,i,c,t)){if(Et(r))return!1;s=!0}}else{let l=n[++a],d=lw(c,i,mf(e),t);if(d===-1){if(Et(r))return!1;s=!0;continue}if(l!==""){let u;if(d>o?u="":u=i[d+1].toLowerCase(),r&2&&l!==u){if(Et(r))return!1;s=!0}}}}return Et(r)||s}function Et(e){return(e&1)===0}function lw(e,n,t,r){if(n===null)return-1;let i=0;if(r||!t){let o=!1;for(;i<n.length;){let s=n[i];if(s===e)return i;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++i];for(;typeof a=="string";)a=n[++i];continue}else{if(s===4)break;if(s===0){i+=4;continue}}i+=o?1:2}return-1}else return fw(n,e)}function lv(e,n,t=!1){for(let r=0;r<n.length;r++)if(cw(e,n[r],t))return!0;return!1}function dw(e){let n=e.attrs;if(n!=null){let t=n.indexOf(5);if((t&1)===0)return n[t+1]}return null}function uw(e){for(let n=0;n<e.length;n++){let t=e[n];if(Ng(t))return n}return e.length}function fw(e,n){let t=e.indexOf(4);if(t>-1)for(t++;t<e.length;){let r=e[t];if(typeof r=="number")return-1;if(r===n)return t;t++}return-1}function pw(e,n){e:for(let t=0;t<n.length;t++){let r=n[t];if(e.length===r.length){for(let i=0;i<e.length;i++)if(e[i]!==r[i])continue e;return!0}}return!1}function sg(e,n){return e?":not("+n.trim()+")":n}function hw(e){let n=e[0],t=1,r=2,i="",o=!1;for(;t<e.length;){let s=e[t];if(typeof s=="string")if(r&2){let a=e[++t];i+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?i+="."+s:r&4&&(i+=" "+s);else i!==""&&!Et(s)&&(n+=sg(o,i),i=""),r=s,o=o||!Et(r);t++}return i!==""&&(n+=sg(o,i)),n}function mw(e){return e.map(hw).join(",")}function gw(e){let n=[],t=[],r=1,i=2;for(;r<e.length;){let o=e[r];if(typeof o=="string")i===2?o!==""&&n.push(o,e[++r]):i===8&&t.push(o);else{if(!Et(i))break;i=o}r++}return t.length&&n.push(1,...t),n}var ct={};function gf(e,n,t,r,i,o,s,a,c,l,d){let u=me+r,p=u+i,h=vw(u,p),g=typeof l=="function"?l():l;return h[x]={type:e,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:n,data:h.slice().fill(null,u),bindingStartIndex:u,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:g,incompleteFirstPass:!1,ssrId:d}}function vw(e,n){let t=[];for(let r=0;r<n;r++)t.push(r<e?null:ct);return t}function yw(e){let n=e.tView;return n===null||n.incompleteFirstPass?e.tView=gf(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):n}function vf(e,n,t,r,i,o,s,a,c,l,d){let u=n.blueprint.slice();return u[vt]=i,u[T]=r|4|128|8|64|1024,(l!==null||e&&e[T]&2048)&&(u[T]|=2048),Wd(u),u[Ee]=u[lr]=e,u[Ae]=t,u[yt]=s||e&&e[yt],u[ie]=a||e&&e[ie],u[In]=c||e&&e[In]||null,u[Ge]=o,u[Mn]=wE(),u[Qr]=d,u[Bd]=l,u[Xe]=n.type==2?e[Xe]:u,u}function bw(e,n,t){let r=_t(n,e),i=yw(t),o=e[yt].rendererFactory,s=yf(e,vf(e,i,null,dv(t),r,n,null,o.createRenderer(r,t),null,null,null));return e[n.index]=s}function dv(e){let n=16;return e.signals?n=4096:e.onPush&&(n=64),n}function uv(e,n,t,r){if(t===0)return-1;let i=n.length;for(let o=0;o<t;o++)n.push(r),e.blueprint.push(r),e.data.push(null);return i}function yf(e,n){return e[Kr]?e[Vd][ot]=n:e[Kr]=n,e[Vd]=n,n}function S(e=1){fv(fe(),R(),Tn()+e,!1)}function fv(e,n,t,r){if(!r)if((n[T]&3)===3){let o=e.preOrderCheckHooks;o!==null&&La(n,o,t)}else{let o=e.preOrderHooks;o!==null&&ja(n,o,0,t)}An(t)}var cc=(function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e})(cc||{});function Lu(e,n,t,r){let i=A(null);try{let[o,s,a]=e.inputs[t],c=null;(s&cc.SignalBased)!==0&&(c=n[o][be]),c!==null&&c.transformFn!==void 0?r=c.transformFn(r):a!==null&&(r=a.call(n,r)),e.setInput!==null?e.setInput(n,c,r,t,o):xg(n,c,o,r)}finally{A(i)}}var $t=(function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e})($t||{}),_w;function bf(e,n){return _w(e,n)}var oj=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var ju=new WeakMap,mo=new WeakSet;function Dw(e,n){let t=ju.get(e);if(!t||t.length===0)return;let r=n.parentNode,i=n.previousSibling;for(let o=t.length-1;o>=0;o--){let s=t[o],a=s.parentNode;s===n?(t.splice(o,1),mo.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(i&&s===i||a&&r&&a!==r)&&(t.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function Cw(e,n){let t=ju.get(e);t?t.includes(n)||t.push(n):ju.set(e,[n])}var ai=new Set,lc=(function(e){return e[e.CHANGE_DETECTION=0]="CHANGE_DETECTION",e[e.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",e})(lc||{}),Gt=new m(""),ag=new Set;function ui(e){ag.has(e)||(ag.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}var _f=(()=>{class e{impl=null;execute(){this.impl?.execute()}static \u0275prov=y({token:e,providedIn:"root",factory:()=>new e})}return e})(),Df=[0,1,2,3],pv=(()=>{class e{ngZone=f($);scheduler=f(jt);errorHandler=f(Ke,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){f(Gt,{optional:!0})}execute(){let t=this.sequences.size>0;t&&X(z.AfterRenderHooksStart),this.executing=!0;for(let r of Df)for(let i of this.sequences)if(!(i.erroredOrDestroyed||!i.hooks[r]))try{i.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=i.hooks[r];return o(i.pipelinedValue)},i.snapshot))}catch(o){i.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),t&&X(z.AfterRenderHooksEnd)}register(t){let{view:r}=t;r!==void 0?((r[ur]??=[]).push(t),hr(r),r[T]|=8192):this.executing?this.deferredRegistrations.add(t):this.addSequence(t)}addSequence(t){this.sequences.add(t),this.scheduler.notify(7)}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}maybeTrace(t,r){return r?r.run(lc.AFTER_NEXT_RENDER,t):t()}static \u0275prov=y({token:e,providedIn:"root",factory:()=>new e})}return e})(),Za=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,t,r,i,o,s=null){this.impl=n,this.hooks=t,this.view=r,this.once=i,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[ur];n&&(this.view[ur]=n.filter(t=>t!==this))}};var Ew=new m("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:f(ue)})});function hv(e,n,t){let r=e.get(Ew);if(Array.isArray(n))for(let i of n)r.queue.add(i),t?.detachedLeaveAnimationFns?.push(i);else r.queue.add(n),t?.detachedLeaveAnimationFns?.push(n);r.scheduler&&r.scheduler(e)}function ww(e,n){for(let[t,r]of n)hv(e,r.animateFns)}function cg(e,n,t,r){let i=e?.[Xr]?.enter;n!==null&&i&&i.has(t.index)&&ww(r,i)}function ri(e,n,t,r,i,o,s,a){if(i!=null){let c,l=!1;bt(i)?c=i:tn(i)&&(l=!0,i=i[vt]);let d=st(i);e===0&&r!==null?(cg(a,r,o,t),s==null?ov(n,r,d):qa(n,r,d,s||null,!0)):e===1&&r!==null?(cg(a,r,o,t),qa(n,r,d,s||null,!0),Dw(o,d)):e===2?(a?.[Xr]?.leave?.has(o.index)&&Cw(o,d),mo.delete(d),lg(a,o,t,u=>{if(mo.has(d)){mo.delete(d);return}tw(n,d,l,u)})):e===3&&(mo.delete(d),lg(a,o,t,()=>{n.destroyNode(d)})),c!=null&&Fw(n,e,t,c,o,r,s)}}function Iw(e,n){mv(e,n),n[vt]=null,n[Ge]=null}function xw(e,n,t,r,i,o){r[vt]=i,r[Ge]=n,dc(e,r,t,1,i,o)}function mv(e,n){n[yt].changeDetectionScheduler?.notify(9),dc(e,n,n[ie],2,null,null)}function Mw(e){let n=e[Kr];if(!n)return vu(e[x],e);for(;n;){let t=null;if(tn(n))t=n[Kr];else{let r=n[ke];r&&(t=r)}if(!t){for(;n&&!n[ot]&&n!==e;)tn(n)&&vu(n[x],n),n=n[Ee];n===null&&(n=e),tn(n)&&vu(n[x],n),t=n&&n[ot]}n=t}}function Cf(e,n){let t=e[fr],r=t.indexOf(n);t.splice(r,1)}function Ef(e,n){if(pr(n))return;let t=n[ie];t.destroyNode&&dc(e,n,t,3,null,null),Mw(n)}function vu(e,n){if(pr(n))return;let t=A(null);try{n[T]&=-129,n[T]|=256,n[et]&&yn(n[et]),Aw(e,n),Tw(e,n),n[x].type===1&&n[ie].destroy();let r=n[xn];if(r!==null&&bt(n[Ee])){r!==n[Ee]&&Cf(r,n);let i=n[Vt];i!==null&&i.detachView(e)}Mu(n)}finally{A(t)}}function lg(e,n,t,r){let i=e?.[Xr];if(i==null||i.leave==null||!i.leave.has(n.index))return r(!1);e&&ai.add(e[Mn]),hv(t,()=>{if(i.leave&&i.leave.has(n.index)){let s=i.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:d}=l();a.push(d)}i.detachedLeaveAnimationFns=void 0}i.running=Promise.allSettled(a),Sw(e,r)}else e&&ai.delete(e[Mn]),r(!1)},i)}function Sw(e,n){let t=e[Xr]?.running;if(t){t.then(()=>{e[Xr].running=void 0,ai.delete(e[Mn]),n(!0)});return}n(!1)}function Tw(e,n){let t=e.cleanup,r=n[Yr];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?r[a]():r[-a].unsubscribe(),s+=2}else{let a=r[t[s+1]];t[s].call(a)}r!==null&&(n[Yr]=null);let i=n[Jt];if(i!==null){n[Jt]=null;for(let s=0;s<i.length;s++){let a=i[s];a()}}let o=n[_n];if(o!==null){n[_n]=null;for(let s of o)s.destroy()}}function Aw(e,n){let t;if(e!=null&&(t=e.destroyHooks)!=null)for(let r=0;r<t.length;r+=2){let i=n[t[r]];if(!(i instanceof vr)){let o=t[r+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=i[o[s]],c=o[s+1];X(z.LifecycleHookStart,a,c);try{c.call(a)}finally{X(z.LifecycleHookEnd,a,c)}}else{X(z.LifecycleHookStart,i,o);try{o.call(i)}finally{X(z.LifecycleHookEnd,i,o)}}}}}function gv(e,n,t){return Rw(e,n.parent,t)}function Rw(e,n,t){let r=n;for(;r!==null&&r.type&168;)n=r,r=n.parent;if(r===null)return t[vt];if(nn(r)){let{encapsulation:i}=e.data[r.directiveStart+r.componentOffset];if(i===It.None||i===It.Emulated)return null}return _t(r,t)}function vv(e,n,t){return Ow(e,n,t)}function Nw(e,n,t){return e.type&40?_t(e,t):null}var Ow=Nw,dg;function wf(e,n,t,r){let i=gv(e,r,n),o=n[ie],s=r.parent||n[Ge],a=vv(s,r,n);if(i!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)og(o,i,t[c],a,!1);else og(o,i,t,a,!1);dg!==void 0&&dg(o,r,n,t,i)}function go(e,n){if(n!==null){let t=n.type;if(t&3)return _t(n,e);if(t&4)return Vu(-1,e[n.index]);if(t&8){let r=n.child;if(r!==null)return go(e,r);{let i=e[n.index];return bt(i)?Vu(-1,i):st(i)}}else{if(t&128)return go(e,n.next);if(t&32)return bf(n,e)()||st(e[n.index]);{let r=yv(e,n);if(r!==null){if(Array.isArray(r))return r[0];let i=Dn(e[Xe]);return go(i,r)}else return go(e,n.next)}}}return null}function yv(e,n){if(n!==null){let r=e[Xe][Ge],i=n.projection;return r.projection[i]}return null}function Vu(e,n){let t=ke+e+1;if(t<n.length){let r=n[t],i=r[x].firstChild;if(i!==null)return go(r,i)}return n[Sn]}function If(e,n,t,r,i,o,s){for(;t!=null;){let a=r[In];if(t.type===128){t=t.next;continue}let c=r[t.index],l=t.type;if(s&&n===0&&(c&&si(st(c),r),t.flags|=2),!sc(t))if(l&8)If(e,n,t.child,r,i,o,!1),ri(n,e,a,i,c,t,o,r);else if(l&32){let d=bf(t,r),u;for(;u=d();)ri(n,e,a,i,u,t,o,r);ri(n,e,a,i,c,t,o,r)}else l&16?bv(e,n,r,t,i,o):ri(n,e,a,i,c,t,o,r);t=s?t.projectionNext:t.next}}function dc(e,n,t,r,i,o){If(t,r,e.firstChild,n,i,o,!1)}function kw(e,n,t){let r=n[ie],i=gv(e,t,n),o=t.parent||n[Ge],s=vv(o,t,n);bv(r,0,n,t,i,s)}function bv(e,n,t,r,i,o){let s=t[Xe],c=s[Ge].projection[r.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];ri(n,e,t[In],i,d,r,o,t)}else{let l=c,d=s[Ee];$g(r)&&(l.flags|=128),If(e,n,l,d,i,o,!0)}}function Fw(e,n,t,r,i,o,s){let a=r[Sn],c=st(r);a!==c&&ri(n,e,t,o,a,i,s);for(let l=ke;l<r.length;l++){let d=r[l];dc(d[x],d,e,n,o,a)}}function Pw(e,n,t,r,i){if(n)i?e.addClass(t,r):e.removeClass(t,r);else{let o=r.indexOf("-")===-1?void 0:$t.DashCase;i==null?e.removeStyle(t,r,o):(typeof i=="string"&&i.endsWith("!important")&&(i=i.slice(0,-10),o|=$t.Important),e.setStyle(t,r,i,o))}}function _v(e,n,t,r,i){let o=Tn(),s=r&2;try{An(-1),s&&n.length>me&&fv(e,n,me,!1);let a=s?z.TemplateUpdateStart:z.TemplateCreateStart;X(a,i,t),t(r,i)}finally{An(o);let a=s?z.TemplateUpdateEnd:z.TemplateCreateEnd;X(a,i,t)}}function uc(e,n,t){Uw(e,n,t),(t.flags&64)===64&&$w(e,n,t)}function xo(e,n,t=_t){let r=n.localNames;if(r!==null){let i=n.index+1;for(let o=0;o<r.length;o+=2){let s=r[o+1],a=s===-1?t(n,e):e[s];e[i++]=a}}}function Lw(e,n,t,r){let o=r.get(Qg,Zg)||t===It.ShadowDom||t===It.ExperimentalIsolatedShadowDom,s=e.selectRootElement(n,o);return jw(s),s}function jw(e){Vw(e)}var Vw=()=>null;function Bw(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function Dv(e,n,t,r,i,o){let s=n[x];if(Tf(e,s,n,t,r)){nn(e)&&Hw(n,e.index);return}e.type&3&&(t=Bw(t)),Cv(e,n,t,r,i,o)}function Cv(e,n,t,r,i,o){if(e.type&3){let s=_t(e,n);r=o!=null?o(r,e.value||"",t):r,i.setProperty(s,t,r)}else e.type&12}function Hw(e,n){let t=at(n,e);t[T]&16||(t[T]|=64)}function Uw(e,n,t){let r=t.directiveStart,i=t.directiveEnd;nn(t)&&bw(n,t,e.data[r+t.componentOffset]),e.firstCreatePass||Wa(t,n);let o=t.initialInputs;for(let s=r;s<i;s++){let a=e.data[s],c=yo(n,e,s,t);if(si(c,n),o!==null&&qw(n,s-r,c,a,t,o),Bt(a)){let l=at(t.index,n);l[Ae]=yo(n,e,s,t)}}}function $w(e,n,t){let r=t.directiveStart,i=t.directiveEnd,o=t.index,s=Om();try{An(o);for(let a=r;a<i;a++){let c=e.data[a],l=n[a];Ma(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&zw(c,l)}}finally{An(-1),Ma(s)}}function zw(e,n){e.hostBindings!==null&&e.hostBindings(1,n)}function xf(e,n){let t=e.directiveRegistry,r=null;if(t)for(let i=0;i<t.length;i++){let o=t[i];lv(n,o.selectors,!1)&&(r??=[],Bt(o)?r.unshift(o):r.push(o))}return r}function Gw(e,n,t,r,i,o){let s=_t(e,n);Ww(n[ie],s,o,e.value,t,r,i)}function Ww(e,n,t,r,i,o,s){if(o==null)e.removeAttribute(n,i,t);else{let a=s==null?ma(o):s(o,r||"",i);e.setAttribute(n,i,a,t)}}function qw(e,n,t,r,i,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Lu(r,t,c,l)}}function Mf(e,n,t,r,i){let o=me+t,s=n[x],a=i(s,n,e,r,t);n[o]=a,ti(e,!0);let c=e.type===2;return c?(sv(n[ie],a,e),(xm()===0||Jr(e))&&si(a,n),Mm()):si(a,n),Ra()&&(!c||!sc(e))&&wf(s,n,a,e),e}function Sf(e){let n=e;return nu()?ru():(n=n.parent,ti(n,!1)),n}function Zw(e,n){let t=e[In];if(!t)return;let r;try{r=t.get(Ct,null)}catch{r=null}r?.(n)}function Tf(e,n,t,r,i){let o=e.inputs?.[r],s=e.hostDirectiveInputs?.[r],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],u=n.data[l];Lu(u,t[l],d,i),a=!0}if(o)for(let c of o){let l=t[c],d=n.data[c];Lu(d,l,r,i),a=!0}return a}function Qw(e,n){let t=at(n,e),r=t[x];Yw(r,t);let i=t[vt];i!==null&&t[Qr]===null&&(t[Qr]=Yg(i,t[In])),X(z.ComponentStart);try{Af(r,t,t[Ae])}finally{X(z.ComponentEnd,t[Ae])}}function Yw(e,n){for(let t=n.length;t<e.blueprint.length;t++)n.push(e.blueprint[t])}function Af(e,n,t){Ta(n);try{let r=e.viewQuery;r!==null&&Su(1,r,t);let i=e.template;i!==null&&_v(e,n,i,1,t),e.firstCreatePass&&(e.firstCreatePass=!1),n[Vt]?.finishViewCreation(e),e.staticContentQueries&&Kg(e,n),e.staticViewQueries&&Su(2,e.viewQuery,t);let o=e.components;o!==null&&Kw(n,o)}catch(r){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),r}finally{n[T]&=-5,Aa()}}function Kw(e,n){for(let t=0;t<n.length;t++)Qw(e,n[t])}function Rf(e,n,t,r){let i=A(null);try{let o=n.tView,a=e[T]&4096?4096:16,c=vf(e,o,t,a,null,n,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),l=e[n.index];c[xn]=l;let d=e[Vt];return d!==null&&(c[Vt]=d.createEmbeddedView(o)),Af(o,c,t),c}finally{A(i)}}function Qa(e,n){return!n||n.firstChild===null||$g(e)}function _o(e,n,t,r,i=!1){for(;t!==null;){if(t.type===128){t=i?t.projectionNext:t.next;continue}let o=n[t.index];o!==null&&r.push(st(o)),bt(o)&&Ev(o,r);let s=t.type;if(s&8)_o(e,n,t.child,r);else if(s&32){let a=bf(t,n),c;for(;c=a();)r.push(c)}else if(s&16){let a=yv(n,t);if(Array.isArray(a))r.push(...a);else{let c=Dn(n[Xe]);_o(c[x],c,a,r,!0)}}t=i?t.projectionNext:t.next}return r}function Ev(e,n){for(let t=ke;t<e.length;t++){let r=e[t],i=r[x].firstChild;i!==null&&_o(r[x],r,i,n)}e[Sn]!==e[vt]&&n.push(e[Sn])}function wv(e){if(e[ur]!==null){for(let n of e[ur])n.impl.addSequence(n);e[ur].length=0}}var Iv=[];function Xw(e){return e[et]??Jw(e)}function Jw(e){let n=Iv.pop()??Object.create(t0);return n.lView=e,n}function e0(e){e.lView[et]!==e&&(e.lView=null,Iv.push(e))}var t0=P(v({},Wn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{hr(e.lView)},consumerOnSignalRead(){this.lView[et]=this}});function n0(e){let n=e[et]??Object.create(r0);return n.lView=e,n}var r0=P(v({},Wn),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:e=>{let n=Dn(e.lView);for(;n&&!xv(n[x]);)n=Dn(n);n&&qd(n)},consumerOnSignalRead(){this.lView[et]=this}});function xv(e){return e.type!==2}function Mv(e){if(e[_n]===null)return;let n=!0;for(;n;){let t=!1;for(let r of e[_n])r.dirty&&(t=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()));n=t&&!!(e[T]&8192)}}var i0=100;function Sv(e,n=0){let r=e[yt].rendererFactory,i=!1;i||r.begin?.();try{o0(e,n)}finally{i||r.end?.()}}function o0(e,n){let t=iu();try{eo(!0),Bu(e,n);let r=0;for(;co(e);){if(r===i0)throw new b(103,!1);r++,Bu(e,1)}}finally{eo(t)}}function s0(e,n,t,r){if(pr(n))return;let i=n[T],o=!1,s=!1;Ta(n);let a=!0,c=null,l=null;o||(xv(e)?(l=Xw(n),c=vn(l)):Is()===null?(a=!1,l=n0(n),c=vn(l)):n[et]&&(yn(n[et]),n[et]=null));try{Wd(n),Am(e.bindingStartIndex),t!==null&&_v(e,n,t,2,r);let d=(i&3)===3;if(!o)if(d){let h=e.preOrderCheckHooks;h!==null&&La(n,h,null)}else{let h=e.preOrderHooks;h!==null&&ja(n,h,0,null),mu(n,0)}if(s||a0(n),Mv(n),Tv(n,0),e.contentQueries!==null&&Kg(e,n),!o)if(d){let h=e.contentCheckHooks;h!==null&&La(n,h)}else{let h=e.contentHooks;h!==null&&ja(n,h,1),mu(n,1)}l0(e,n);let u=e.components;u!==null&&Rv(n,u,0);let p=e.viewQuery;if(p!==null&&Su(2,p,r),!o)if(d){let h=e.viewCheckHooks;h!==null&&La(n,h)}else{let h=e.viewHooks;h!==null&&ja(n,h,2),mu(n,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),n[_a]){for(let h of n[_a])h();n[_a]=null}o||(wv(n),n[T]&=-73)}catch(d){throw o||hr(n),d}finally{l!==null&&(Zn(l,c),a&&e0(l)),Aa()}}function Tv(e,n){for(let t=Gg(e);t!==null;t=Wg(t))for(let r=ke;r<t.length;r++){let i=t[r];Av(i,n)}}function a0(e){for(let n=Gg(e);n!==null;n=Wg(n)){if(!(n[T]&2))continue;let t=n[fr];for(let r=0;r<t.length;r++){let i=t[r];qd(i)}}}function c0(e,n,t){X(z.ComponentStart);let r=at(n,e);try{Av(r,t)}finally{X(z.ComponentEnd,r[Ae])}}function Av(e,n){Ca(e)&&Bu(e,n)}function Bu(e,n){let r=e[x],i=e[T],o=e[et],s=!!(n===0&&i&16);if(s||=!!(i&64&&n===0),s||=!!(i&1024),s||=!!(o?.dirty&&kr(o)),s||=!1,o&&(o.dirty=!1),e[T]&=-9217,s)s0(r,e,r.template,e[Ae]);else if(i&8192){let a=A(null);try{Mv(e),Tv(e,1);let c=r.components;c!==null&&Rv(e,c,1),wv(e)}finally{A(a)}}}function Rv(e,n,t){for(let r=0;r<n.length;r++)c0(e,n[r],t)}function l0(e,n){let t=e.hostBindingOpCodes;if(t!==null)try{for(let r=0;r<t.length;r++){let i=t[r];if(i<0)An(~i);else{let o=i,s=t[++r],a=t[++r];Nm(s,o);let c=n[o];X(z.HostBindingsUpdateStart,c);try{a(2,c)}finally{X(z.HostBindingsUpdateEnd,c)}}}}finally{An(-1)}}function Nf(e,n){let t=iu()?64:1088;for(e[yt].changeDetectionScheduler?.notify(n);e;){e[T]|=t;let r=Dn(e);if(ei(e)&&!r)return e;e=r}return null}function Nv(e,n,t,r){return[e,!0,0,n,null,r,null,t,null,null]}function d0(e,n){let t=ke+n;if(t<e.length)return e[t]}function Of(e,n,t,r=!0){let i=n[x];if(f0(i,n,e,t),r){let s=Vu(t,e),a=n[ie],c=a.parentNode(e[Sn]);c!==null&&xw(i,e[Ge],a,n,c,s)}let o=n[Qr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function u0(e,n){let t=Ya(e,n);return t!==void 0&&Ef(t[x],t),t}function Ya(e,n){if(e.length<=ke)return;let t=ke+n,r=e[t];if(r){let i=r[xn];i!==null&&i!==e&&Cf(i,r),n>0&&(e[t-1][ot]=r[ot]);let o=oo(e,ke+n);Iw(r[x],r);let s=o[Vt];s!==null&&s.detachView(o[x]),r[Ee]=null,r[ot]=null,r[T]&=-129}return r}function f0(e,n,t,r){let i=ke+r,o=t.length;r>0&&(t[i-1][ot]=n),r<o-ke?(n[ot]=t[i],Od(t,ke+r,n)):(t.push(n),n[ot]=null),n[Ee]=t;let s=n[xn];s!==null&&t!==s&&Ov(s,n);let a=n[Vt];a!==null&&a.insertView(e),Ea(n),n[T]|=128}function Ov(e,n){let t=e[fr],r=n[Ee];if(tn(r))e[T]|=2;else{let i=r[Ee][Xe];n[Xe]!==i&&(e[T]|=2)}t===null?e[fr]=[n]:t.push(n)}var Nn=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,t=n[x];return _o(t,n,t.firstChild,[])}constructor(n,t){this._lView=n,this._cdRefInjectingView=t}get context(){return this._lView[Ae]}set context(n){this._lView[Ae]=n}get destroyed(){return pr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Ee];if(bt(n)){let t=n[ao],r=t?t.indexOf(this):-1;r>-1&&(Ya(n,r),oo(t,r))}this._attachedToViewContainer=!1}Ef(this._lView[x],this._lView)}onDestroy(n){Zd(this._lView,n)}markForCheck(){Nf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[T]&=-129}reattach(){Ea(this._lView),this._lView[T]|=128}detectChanges(){this._lView[T]|=1024,Sv(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new b(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=ei(this._lView),t=this._lView[xn];t!==null&&!n&&Cf(t,this._lView),mv(this._lView[x],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new b(902,!1);this._appRef=n;let t=ei(this._lView),r=this._lView[xn];r!==null&&!t&&Ov(r,this._lView),Ea(this._lView)}};var yr=(()=>{class e{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=p0;constructor(t,r,i){this._declarationLView=t,this._declarationTContainer=r,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,r){return this.createEmbeddedViewImpl(t,r)}createEmbeddedViewImpl(t,r,i){let o=Rf(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:r,dehydratedView:i});return new Nn(o)}}return e})();function p0(){return fc(ye(),R())}function fc(e,n){return e.type&4?new yr(n,e,li(e,n)):null}function fi(e,n,t,r,i){let o=e.data[n];if(o===null)o=h0(e,n,t,r,i),Rm()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=r,o.attrs=i;let s=Sm();o.injectorIndex=s===null?-1:s.injectorIndex}return ti(o,!0),o}function h0(e,n,t,r,i){let o=tu(),s=nu(),a=s?o:o&&o.parent,c=e.data[n]=g0(e,a,t,n,r,i);return m0(e,c,o,s),c}function m0(e,n,t,r){e.firstChild===null&&(e.firstChild=n),t!==null&&(r?t.child==null&&n.parent!==null&&(t.child=n):t.next===null&&(t.next=n,n.prev=t))}function g0(e,n,t,r,i,o){let s=n?n.injectorIndex:-1,a=0;return Xd()&&(a|=128),{type:t,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:i,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var v0=()=>null,y0=()=>null;function Hu(e,n){return v0(e,n)}function b0(e,n,t){return y0(e,n,t)}var kv=class{},pc=class{},Uu=class{resolveComponentFactory(n){throw new b(917,!1)}},Mo=class{static NULL=new Uu},xt=class{},St=(()=>{class e{destroyNode=null;static __NG_ELEMENT_ID__=()=>_0()}return e})();function _0(){let e=R(),n=ye(),t=at(n.index,e);return(tn(t)?t:e)[ie]}var Fv=(()=>{class e{static \u0275prov=y({token:e,providedIn:"root",factory:()=>null})}return e})();var Ba={},$u=class{injector;parentInjector;constructor(n,t){this.injector=n,this.parentInjector=t}get(n,t,r){let i=this.injector.get(n,Ba,r);return i!==Ba||t===Ba?i:this.parentInjector.get(n,t,r)}};function Ka(e,n,t){let r=t?e.styles:null,i=t?e.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)i=fa(i,a);else if(o==2){let c=a,l=n[++s];r=fa(r,c+": "+l+";")}}t?e.styles=r:e.stylesWithoutHost=r,t?e.classes=i:e.classesWithoutHost=i}function oe(e,n=0){let t=R();if(t===null)return E(e,n);let r=ye();return Vg(r,t,Te(e),n)}function Pv(e,n,t,r,i){let o=r===null?null:{"":-1},s=i(e,t);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}E0(e,n,t,a,o,c,l)}o!==null&&r!==null&&D0(t,r,o)}function D0(e,n,t){let r=e.localNames=[];for(let i=0;i<n.length;i+=2){let o=t[n[i+1]];if(o==null)throw new b(-301,!1);r.push(n[i],o)}}function C0(e,n,t){n.componentOffset=t,(e.components??=[]).push(n.index)}function E0(e,n,t,r,i,o,s){let a=r.length,c=null;for(let p=0;p<a;p++){let h=r[p];c===null&&Bt(h)&&(c=h,C0(e,t,p)),Iu(Wa(t,n),e,h.type)}T0(t,e.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let p=0;p<a;p++){let h=r[p];h.providersResolver&&h.providersResolver(h)}let l=!1,d=!1,u=uv(e,n,a,null);a>0&&(t.directiveToIndex=new Map);for(let p=0;p<a;p++){let h=r[p];if(t.mergedAttrs=oi(t.mergedAttrs,h.hostAttrs),I0(e,t,n,u,h),S0(u,h,i),s!==null&&s.has(h)){let[I,D]=s.get(h);t.directiveToIndex.set(h.type,[u,I+t.directiveStart,D+t.directiveStart])}else(o===null||!o.has(h))&&t.directiveToIndex.set(h.type,u);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let g=h.type.prototype;!l&&(g.ngOnChanges||g.ngOnInit||g.ngDoCheck)&&((e.preOrderHooks??=[]).push(t.index),l=!0),!d&&(g.ngOnChanges||g.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(t.index),d=!0),u++}w0(e,t,o)}function w0(e,n,t){for(let r=n.directiveStart;r<n.directiveEnd;r++){let i=e.data[r];if(t===null||!t.has(i))ug(0,n,i,r),ug(1,n,i,r),pg(n,r,!1);else{let o=t.get(i);fg(0,n,o,r),fg(1,n,o,r),pg(n,r,!0)}}}function ug(e,n,t,r){let i=e===0?t.inputs:t.outputs;for(let o in i)if(i.hasOwnProperty(o)){let s;e===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(r),Lv(n,o)}}function fg(e,n,t,r){let i=e===0?t.inputs:t.outputs;for(let o in i)if(i.hasOwnProperty(o)){let s=i[o],a;e===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(r,o),Lv(n,s)}}function Lv(e,n){n==="class"?e.flags|=8:n==="style"&&(e.flags|=16)}function pg(e,n,t){let{attrs:r,inputs:i,hostDirectiveInputs:o}=e;if(r===null||!t&&i===null||t&&o===null||mf(e)){e.initialInputs??=[],e.initialInputs.push(null);return}let s=null,a=0;for(;a<r.length;){let c=r[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&i.hasOwnProperty(c)){let l=i[c];for(let d of l)if(d===n){s??=[],s.push(c,r[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],r[a+1]);break}}a+=2}e.initialInputs??=[],e.initialInputs.push(s)}function I0(e,n,t,r,i){e.data[r]=i;let o=i.factory||(i.factory=bn(i.type,!0)),s=new vr(o,Bt(i),oe,null);e.blueprint[r]=s,t[r]=s,x0(e,n,r,uv(e,t,i.hostVars,ct),i)}function x0(e,n,t,r,i){let o=i.hostBindings;if(o){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~n.index;M0(s)!=a&&s.push(a),s.push(t,r,o)}}function M0(e){let n=e.length;for(;n>0;){let t=e[--n];if(typeof t=="number"&&t<0)return t}return 0}function S0(e,n,t){if(t){if(n.exportAs)for(let r=0;r<n.exportAs.length;r++)t[n.exportAs[r]]=e;Bt(n)&&(t[""]=e)}}function T0(e,n,t){e.flags|=1,e.directiveStart=n,e.directiveEnd=n+t,e.providerIndexes=n}function kf(e,n,t,r,i,o,s,a){let c=n[x],l=c.consts,d=Ht(l,s),u=fi(c,e,t,r,d);return o&&Pv(c,n,u,Ht(l,a),i),u.mergedAttrs=oi(u.mergedAttrs,u.attrs),u.attrs!==null&&Ka(u,u.attrs,!1),u.mergedAttrs!==null&&Ka(u,u.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,u),u}function Ff(e,n){Ag(e,n),Hd(n)&&e.queries.elementEnd(n)}function A0(e,n,t,r,i,o){let s=n.consts,a=Ht(s,i),c=fi(n,e,t,r,a);if(c.mergedAttrs=oi(c.mergedAttrs,c.attrs),o!=null){let l=Ht(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Ka(c,c.attrs,!1),c.mergedAttrs!==null&&Ka(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function jv(e,n,t){return e[n]=t}function Mt(e,n,t){if(t===ct)return!1;let r=e[n];return Object.is(r,t)?!1:(e[n]=t,!0)}function hg(e,n,t,r){let i=Mt(e,n,t);return Mt(e,n+1,r)||i}function R0(e,n,t,r,i,o){let s=hg(e,n,t,r);return hg(e,n+2,i,o)||s}function Ha(e,n,t){return function r(i){let o=nn(e)?at(e.index,n):n;Nf(o,5);let s=n[Ae],a=mg(n,s,t,i),c=r.__ngNextListenerFn__;for(;c;)a=mg(n,s,c,i)&&a,c=c.__ngNextListenerFn__;return a}}function mg(e,n,t,r){let i=A(null);try{return X(z.OutputStart,n,t),t(r)!==!1}catch(o){return Zw(e,o),!1}finally{X(z.OutputEnd,n,t),A(i)}}function Vv(e,n,t,r,i,o,s,a){let c=Jr(e),l=!1,d=null;if(!r&&c&&(d=O0(n,t,o,e.index)),d!==null){let u=d.__ngLastListenerFn__||d;u.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let u=_t(e,t),p=r?r(u):u;TE(t,p,o,a);let h=i.listen(p,o,a);if(!N0(o)){let g=r?I=>r(st(I[e.index])):e.index;Bv(g,n,t,o,a,h,!1)}}return l}function N0(e){return e.startsWith("animation")||e.startsWith("transition")}function O0(e,n,t,r){let i=e.cleanup;if(i!=null)for(let o=0;o<i.length-1;o+=2){let s=i[o];if(s===t&&i[o+1]===r){let a=n[Yr],c=i[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function Bv(e,n,t,r,i,o,s){let a=n.firstCreatePass?Yd(n):null,c=Qd(t),l=c.length;c.push(i,o),a&&a.push(r,e,l,(l+1)*(s?-1:1))}function gg(e,n,t,r,i,o){let s=n[t],a=n[x],l=a.data[t].outputs[r],u=s[l].subscribe(o);Bv(e.index,a,n,i,o,u,!0)}var zu=Symbol("BINDING");function Hv(e){return e.debugInfo?.className||e.type.name||null}var Xa=class extends Mo{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let t=en(n);return new br(t,this.ngModule)}};function k0(e){return Object.keys(e).map(n=>{let[t,r,i]=e[n],o={propName:t,templateName:n,isSignal:(r&cc.SignalBased)!==0};return i&&(o.transform=i),o})}function F0(e){return Object.keys(e).map(n=>({propName:e[n],templateName:n}))}function P0(e,n,t){let r=n instanceof ue?n:n?.injector;return r&&e.getStandaloneInjector!==null&&(r=e.getStandaloneInjector(r)||r),r?new $u(t,r):t}function L0(e){let n=e.get(xt,null);if(n===null)throw new b(407,!1);let t=e.get(Fv,null),r=e.get(jt,null),i=e.get(Gt,null,{optional:!0});return{rendererFactory:n,sanitizer:t,changeDetectionScheduler:r,ngReflect:!1,tracingService:i}}function j0(e,n){let t=Uv(e);return iv(n,t,t==="svg"?Ud:t==="math"?_m:null)}function Uv(e){return(e.selectors[0][0]||"div").toLowerCase()}var br=class extends pc{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=k0(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=F0(this.componentDef.outputs),this.cachedOutputs}constructor(n,t){super(),this.componentDef=n,this.ngModule=t,this.componentType=n.type,this.selector=mw(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!t}create(n,t,r,i,o,s){X(z.DynamicComponentStart);let a=A(null);try{let c=this.componentDef,l=P0(c,i||this.ngModule,n),d=L0(l),u=d.tracingService;return u&&u.componentCreate?u.componentCreate(Hv(c),()=>this.createComponentRef(d,l,t,r,o,s)):this.createComponentRef(d,l,t,r,o,s)}finally{A(a)}}createComponentRef(n,t,r,i,o,s){let a=this.componentDef,c=V0(i,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=i?Lw(l,i,a.encapsulation,t):j0(a,l),u=s?.some(vg)||o?.some(g=>typeof g!="function"&&g.bindings.some(vg)),p=vf(null,c,null,512|dv(a),null,null,n,l,t,null,Yg(d,t,!0));p[me]=d,Ta(p);let h=null;try{let g=kf(me,p,2,"#host",()=>c.directiveRegistry,!0,0);sv(l,d,g),si(d,p),uc(c,p,g),af(c,g,p),Ff(c,g),r!==void 0&&H0(g,this.ngContentSelectors,r),h=at(g.index,p),p[Ae]=h[Ae],Af(c,p,null)}catch(g){throw h!==null&&Mu(h),Mu(p),g}finally{X(z.DynamicComponentEnd),Aa()}return new Ja(this.componentType,p,!!u)}};function V0(e,n,t,r){let i=e?["ng-version","21.2.9"]:gw(n.selectors[0]),o=null,s=null,a=0;if(t)for(let d of t)a+=d[zu].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(r)for(let d=0;d<r.length;d++){let u=r[d];if(typeof u!="function")for(let p of u.bindings){a+=p[zu].requiredVars;let h=d+1;p.create&&(p.targetIdx=h,(o??=[]).push(p)),p.update&&(p.targetIdx=h,(s??=[]).push(p))}}let c=[n];if(r)for(let d of r){let u=typeof d=="function"?d:d.type,p=Ad(u);c.push(p)}return gf(0,null,B0(o,s),1,a,c,null,null,null,[i],null)}function B0(e,n){return!e&&!n?null:t=>{if(t&1&&e)for(let r of e)r.create();if(t&2&&n)for(let r of n)r.update()}}function vg(e){let n=e[zu].kind;return n==="input"||n==="twoWay"}var Ja=class extends kv{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,t,r){super(),this._rootLView=t,this._hasInputBindings=r,this._tNode=Da(t[x],me),this.location=li(this._tNode,t),this.instance=at(this._tNode.index,t)[Ae],this.hostView=this.changeDetectorRef=new Nn(t,void 0),this.componentType=n}setInput(n,t){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),t))return;let i=this._rootLView,o=Tf(r,i[x],i,n,t);this.previousInputValues.set(n,t);let s=at(r.index,i);Nf(s,1)}get injector(){return new gr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function H0(e,n,t){let r=e.projection=[];for(let i=0;i<n.length;i++){let o=t[i];r.push(o!=null&&o.length?Array.from(o):null)}}var ln=(()=>{class e{static __NG_ELEMENT_ID__=U0}return e})();function U0(){let e=ye();return $v(e,R())}var Gu=class e extends ln{_lContainer;_hostTNode;_hostLView;constructor(n,t,r){super(),this._lContainer=n,this._hostTNode=t,this._hostLView=r}get element(){return li(this._hostTNode,this._hostLView)}get injector(){return new gr(this._hostTNode,this._hostLView)}get parentInjector(){let n=tf(this._hostTNode,this._hostLView);if(Og(n)){let t=za(n,this._hostLView),r=$a(n),i=t[x].data[r+8];return new gr(i,t)}else return new gr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let t=yg(this._lContainer);return t!==null&&t[n]||null}get length(){return this._lContainer.length-ke}createEmbeddedView(n,t,r){let i,o;typeof r=="number"?i=r:r!=null&&(i=r.index,o=r.injector);let s=Hu(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,i,Qa(this._hostTNode,s)),a}createComponent(n,t,r,i,o,s,a){let c=n&&!nE(n),l;if(c)l=t;else{let D=t||{};l=D.index,r=D.injector,i=D.projectableNodes,o=D.environmentInjector||D.ngModuleRef,s=D.directives,a=D.bindings}let d=c?n:new br(en(n)),u=r||this.parentInjector;if(!o&&d.ngModule==null){let w=(c?u:this.parentInjector).get(ue,null);w&&(o=w)}let p=en(d.componentType??{}),h=Hu(this._lContainer,p?.id??null),g=h?.firstChild??null,I=d.create(u,i,g,o,s,a);return this.insertImpl(I.hostView,l,Qa(this._hostTNode,h)),I}insert(n,t){return this.insertImpl(n,t,!0)}insertImpl(n,t,r){let i=n._lView;if(Cm(i)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=i[Ee],l=new e(c,c[Ge],c[Ee]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(t),s=this._lContainer;return Of(s,i,o,r),n.attachToViewContainerRef(),Od(yu(s),o,n),n}move(n,t){return this.insert(n,t)}indexOf(n){let t=yg(this._lContainer);return t!==null?t.indexOf(n):-1}remove(n){let t=this._adjustIndex(n,-1),r=Ya(this._lContainer,t);r&&(oo(yu(this._lContainer),t),Ef(r[x],r))}detach(n){let t=this._adjustIndex(n,-1),r=Ya(this._lContainer,t);return r&&oo(yu(this._lContainer),t)!=null?new Nn(r):null}_adjustIndex(n,t=0){return n??this.length+t}};function yg(e){return e[ao]}function yu(e){return e[ao]||(e[ao]=[])}function $v(e,n){let t,r=n[e.index];return bt(r)?t=r:(t=Nv(r,n,null,e),n[e.index]=t,yf(n,t)),z0(t,n,e,r),new Gu(t,e,n)}function $0(e,n){let t=e[ie],r=t.createComment(""),i=_t(n,e),o=t.parentNode(i);return qa(t,o,r,t.nextSibling(i),!1),r}var z0=q0,G0=()=>!1;function W0(e,n,t){return G0(e,n,t)}function q0(e,n,t,r){if(e[Sn])return;let i;t.type&8?i=st(r):i=$0(n,t),e[Sn]=i}var Wu=class e{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new e(this.queryList)}setDirty(){this.queryList.setDirty()}},qu=class e{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let t=n.queries;if(t!==null){let r=n.contentQueries!==null?n.contentQueries[0]:t.length,i=[];for(let o=0;o<r;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];i.push(a.clone())}return new e(i)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let t=0;t<this.queries.length;t++)Lf(n,t).matches!==null&&this.queries[t].setDirty()}},ec=class{flags;read;predicate;constructor(n,t,r=null){this.flags=t,this.read=r,typeof n=="string"?this.predicate=X0(n):this.predicate=n}},Zu=class e{queries;constructor(n=[]){this.queries=n}elementStart(n,t){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(n,t)}elementEnd(n){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(n)}embeddedTView(n){let t=null;for(let r=0;r<this.length;r++){let i=t!==null?t.length:0,o=this.getByIndex(r).embeddedTView(n,i);o&&(o.indexInDeclarationView=r,t!==null?t.push(o):t=[o])}return t!==null?new e(t):null}template(n,t){for(let r=0;r<this.queries.length;r++)this.queries[r].template(n,t)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Qu=class e{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,t=-1){this.metadata=n,this._declarationNodeIndex=t}elementStart(n,t){this.isApplyingToNode(t)&&this.matchTNode(n,t)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,t){this.elementStart(n,t)}embeddedTView(n,t){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,t),new e(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,r=n.parent;for(;r!==null&&r.type&8&&r.index!==t;)r=r.parent;return t===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(n,t){let r=this.metadata.predicate;if(Array.isArray(r))for(let i=0;i<r.length;i++){let o=r[i];this.matchTNodeWithReadOption(n,t,Z0(t,o)),this.matchTNodeWithReadOption(n,t,Va(t,n,o,!1,!1))}else r===yr?t.type&4&&this.matchTNodeWithReadOption(n,t,-1):this.matchTNodeWithReadOption(n,t,Va(t,n,r,!1,!1))}matchTNodeWithReadOption(n,t,r){if(r!==null){let i=this.metadata.read;if(i!==null)if(i===J||i===ln||i===yr&&t.type&4)this.addMatch(t.index,-2);else{let o=Va(t,n,i,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,r)}}addMatch(n,t){this.matches===null?this.matches=[n,t]:this.matches.push(n,t)}};function Z0(e,n){let t=e.localNames;if(t!==null){for(let r=0;r<t.length;r+=2)if(t[r]===n)return t[r+1]}return null}function Q0(e,n){return e.type&11?li(e,n):e.type&4?fc(e,n):null}function Y0(e,n,t,r){return t===-1?Q0(n,e):t===-2?K0(e,n,r):yo(e,e[x],t,n)}function K0(e,n,t){if(t===J)return li(n,e);if(t===yr)return fc(n,e);if(t===ln)return $v(n,e)}function zv(e,n,t,r){let i=n[Vt].queries[r];if(i.matches===null){let o=e.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(Y0(n,d,s[c+1],t.metadata.read))}}i.matches=a}return i.matches}function Yu(e,n,t,r){let i=e.queries.getByIndex(t),o=i.matches;if(o!==null){let s=zv(e,n,i,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)r.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let u=ke;u<d.length;u++){let p=d[u];p[xn]===p[Ee]&&Yu(p[x],p,l,r)}if(d[fr]!==null){let u=d[fr];for(let p=0;p<u.length;p++){let h=u[p];Yu(h[x],h,l,r)}}}}}return r}function Pf(e,n){return e[Vt].queries[n].queryList}function Gv(e,n,t){let r=new bo((t&4)===4);return Im(e,n,r,r.destroy),(n[Vt]??=new qu).queries.push(new Wu(r))-1}function Wv(e,n,t){let r=fe();return r.firstCreatePass&&(Zv(r,new ec(e,n,t),-1),(n&2)===2&&(r.staticViewQueries=!0)),Gv(r,R(),n)}function qv(e,n,t,r){let i=fe();if(i.firstCreatePass){let o=ye();Zv(i,new ec(n,t,r),o.index),J0(i,e),(t&2)===2&&(i.staticContentQueries=!0)}return Gv(i,R(),t)}function X0(e){return e.split(",").map(n=>n.trim())}function Zv(e,n,t){e.queries===null&&(e.queries=new Zu),e.queries.track(new Qu(n,t))}function J0(e,n){let t=e.contentQueries||(e.contentQueries=[]),r=t.length?t[t.length-1]:-1;n!==r&&t.push(e.queries.length-1,n)}function Lf(e,n){return e.queries.getByIndex(n)}function Qv(e,n){let t=e[x],r=Lf(t,n);return r.crossesNgTemplate?Yu(t,e,n,[]):zv(t,e,r,n)}function Yv(e,n,t){let r,i=Hi(()=>{r._dirtyCounter();let o=eI(r,e);if(n&&o===void 0)throw new b(-951,!1);return o});return r=i[be],r._dirtyCounter=We(0),r._flatValue=void 0,i}function jf(e){return Yv(!0,!1,e)}function Vf(e){return Yv(!0,!0,e)}function Kv(e,n){let t=e[be];t._lView=R(),t._queryIndex=n,t._queryList=Pf(t._lView,n),t._queryList.onDirty(()=>t._dirtyCounter.update(r=>r+1))}function eI(e,n){let t=e._lView,r=e._queryIndex;if(t===void 0||r===void 0||t[T]&4)return n?void 0:Oe;let i=Pf(t,r),o=Qv(t,r);return i.reset(o,Ug),n?i.first:i._changesDetected||e._flatValue===void 0?e._flatValue=i.toArray():e._flatValue}var _r=class{},hc=class{};var tc=class extends _r{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Xa(this);constructor(n,t,r,i=!0){super(),this.ngModuleType=n,this._parent=t;let o=Td(n);this._bootstrapComponents=av(o.bootstrap),this._r3Injector=lu(n,t,[{provide:_r,useValue:this},{provide:Mo,useValue:this.componentFactoryResolver},...r],ro(n),new Set(["environment"])),i&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},nc=class extends hc{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new tc(this.moduleType,n,[])}};var Do=class extends _r{injector;componentFactoryResolver=new Xa(this);instance=null;constructor(n){super();let t=new or([...n.providers,{provide:_r,useValue:this},{provide:Mo,useValue:this.componentFactoryResolver}],n.parent||Zr(),n.debugName,new Set(["environment"]));this.injector=t,n.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function So(e,n,t=null){return new Do({providers:e,parent:n,debugName:t,runEnvironmentInitializers:!0}).injector}var tI=(()=>{class e{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let r=Pd(!1,t.type),i=r.length>0?So([r],this._injector,""):null;this.cachedInjectors.set(t,i)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=y({token:e,providedIn:"environment",factory:()=>new e(E(ue))})}return e})();function W(e){return Eo(()=>{let n=Xv(e),t=P(v({},n),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===rf.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&e.dependencies||null,getStandaloneInjector:n.standalone?i=>i.get(tI).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||It.Emulated,styles:e.styles||Oe,_:null,schemas:e.schemas||null,tView:null,id:""});n.standalone&&ui("NgStandalone"),Jv(t);let r=e.dependencies;return t.directiveDefs=bg(r,nI),t.pipeDefs=bg(r,sm),t.id=oI(t),t})}function nI(e){return en(e)||Ad(e)}function Z(e){return Eo(()=>({type:e.type,bootstrap:e.bootstrap||Oe,declarations:e.declarations||Oe,imports:e.imports||Oe,exports:e.exports||Oe,transitiveCompileScopes:null,schemas:e.schemas||null,id:e.id||null}))}function rI(e,n){if(e==null)return wn;let t={};for(let r in e)if(e.hasOwnProperty(r)){let i=e[r],o,s,a,c;Array.isArray(i)?(a=i[0],o=i[1],s=i[2]??o,c=i[3]||null):(o=i,s=i,a=cc.None,c=null),t[o]=[r,a,c],n[o]=s}return t}function iI(e){if(e==null)return wn;let n={};for(let t in e)e.hasOwnProperty(t)&&(n[e[t]]=t);return n}function Q(e){return Eo(()=>{let n=Xv(e);return Jv(n),n})}function To(e){return{type:e.type,name:e.name,factory:null,pure:e.pure!==!1,standalone:e.standalone??!0,onDestroy:e.type.prototype.ngOnDestroy||null}}function Xv(e){let n={};return{type:e.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:n,inputConfig:e.inputs||wn,exportAs:e.exportAs||null,standalone:e.standalone??!0,signals:e.signals===!0,selectors:e.selectors||Oe,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:rI(e.inputs,n),outputs:iI(e.outputs),debugInfo:null}}function Jv(e){e.features?.forEach(n=>n(e))}function bg(e,n){return e?()=>{let t=typeof e=="function"?e():e,r=[];for(let i of t){let o=n(i);o!==null&&r.push(o)}return r}:null}function oI(e){let n=0,t=typeof e.consts=="function"?"":e.consts,r=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,t,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery];for(let o of r.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function sI(e){return Object.getPrototypeOf(e.prototype).constructor}function lt(e){let n=sI(e.type),t=!0,r=[e];for(;n;){let i;if(Bt(e))i=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new b(903,!1);i=n.\u0275dir}if(i){if(t){r.push(i);let s=e;s.inputs=bu(e.inputs),s.declaredInputs=bu(e.declaredInputs),s.outputs=bu(e.outputs);let a=i.hostBindings;a&&uI(e,a);let c=i.viewQuery,l=i.contentQueries;if(c&&lI(e,c),l&&dI(e,l),aI(e,i),om(e.outputs,i.outputs),Bt(i)&&i.data.animation){let d=e.data;d.animation=(d.animation||[]).concat(i.data.animation)}}let o=i.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(e),a===lt&&(t=!1)}}n=Object.getPrototypeOf(n)}cI(r)}function aI(e,n){for(let t in n.inputs){if(!n.inputs.hasOwnProperty(t)||e.inputs.hasOwnProperty(t))continue;let r=n.inputs[t];r!==void 0&&(e.inputs[t]=r,e.declaredInputs[t]=n.declaredInputs[t])}}function cI(e){let n=0,t=null;for(let r=e.length-1;r>=0;r--){let i=e[r];i.hostVars=n+=i.hostVars,i.hostAttrs=oi(i.hostAttrs,t=oi(t,i.hostAttrs))}}function bu(e){return e===wn?{}:e===Oe?[]:e}function lI(e,n){let t=e.viewQuery;t?e.viewQuery=(r,i)=>{n(r,i),t(r,i)}:e.viewQuery=n}function dI(e,n){let t=e.contentQueries;t?e.contentQueries=(r,i,o)=>{n(r,i,o),t(r,i,o)}:e.contentQueries=n}function uI(e,n){let t=e.hostBindings;t?e.hostBindings=(r,i)=>{n(r,i),t(r,i)}:e.hostBindings=n}function ey(e,n,t,r,i,o,s,a){if(t.firstCreatePass){e.mergedAttrs=oi(e.mergedAttrs,e.attrs);let d=e.tView=gf(2,e,i,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,e),d.queries=t.queries.embeddedTView(e))}a&&(e.flags|=a),ti(e,!1);let c=pI(t,n,e,r);Ra()&&wf(t,n,c,e),si(c,n);let l=Nv(c,n,c,e);n[r+me]=l,yf(n,l),W0(l,e,n)}function fI(e,n,t,r,i,o,s,a,c,l,d){let u=t+me,p;return n.firstCreatePass?(p=fi(n,u,4,s||null,a||null),wa()&&Pv(n,e,p,Ht(n.consts,l),xf),Ag(n,p)):p=n.data[u],ey(p,e,n,t,r,i,o,c),Jr(p)&&uc(n,e,p),l!=null&&xo(e,p,d),p}function Bf(e,n,t,r,i,o,s,a,c,l,d){let u=t+me,p;if(n.firstCreatePass){if(p=fi(n,u,4,s||null,a||null),l!=null){let h=Ht(n.consts,l);p.localNames=[];for(let g=0;g<h.length;g+=2)p.localNames.push(h[g],-1)}}else p=n.data[u];return ey(p,e,n,t,r,i,o,c),l!=null&&xo(e,p,d),p}function Wt(e,n,t,r,i,o,s,a){let c=R(),l=fe(),d=Ht(l.consts,o);return fI(c,l,e,n,t,r,i,d,void 0,s,a),Wt}var pI=hI;function hI(e,n,t,r){return fo(!0),n[ie].createComment("")}var mc=(()=>{class e{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"platform"})}return e})();function Hf(e){return typeof e=="function"&&e[be]!==void 0}function Uf(e){return Hf(e)&&typeof e.set=="function"}var $f=new m("");function kn(e){return!!e&&typeof e.then=="function"}function zf(e){return!!e&&typeof e.subscribe=="function"}var ty=new m("");var Gf=(()=>{class e{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,r)=>{this.resolve=t,this.reject=r});appInits=f(ty,{optional:!0})??[];injector=f(he);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let i of this.appInits){let o=we(this.injector,i);if(kn(o))t.push(o);else if(zf(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{r()}).catch(i=>{this.reject(i)}),t.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),gc=new m("");function ny(){Ul(()=>{let e="";throw new b(600,e)})}function ry(e){return e.isBoundToModule}var mI=10;var qt=(()=>{class e{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=f(Ct);afterRenderManager=f(_f);zonelessEnabled=f(ho);rootEffectScheduler=f(Oa);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new H;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=f(rn);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(L(t=>!t))}constructor(){f(Gt,{optional:!0})}whenStable(){let t;return new Promise(r=>{t=this.isStable.subscribe({next:i=>{i&&r()}})}).finally(()=>{t.unsubscribe()})}_injector=f(ue);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,r){return this.bootstrapImpl(t,r)}bootstrapImpl(t,r,i=he.NULL){return this._injector.get($).run(()=>{X(z.BootstrapComponentStart);let s=t instanceof pc;if(!this._injector.get(Gf).done){let g="";throw new b(405,g)}let c;s?c=t:c=this._injector.get(Mo).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=ry(c)?void 0:this._injector.get(_r),d=r||c.selector,u=c.create(i,[],d,l),p=u.location.nativeElement,h=u.injector.get($f,null);return h?.registerApplication(p),u.onDestroy(()=>{this.detachView(u.hostView),vo(this.components,u),h?.unregisterApplication(p)}),this._loadComponent(u),X(z.BootstrapComponentEnd,u),u})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){X(z.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(lc.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw X(z.ChangeDetectionEnd),new b(101,!1);let t=A(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,A(t),this.afterTick.next(),X(z.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(xt,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<mI;){X(z.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{X(z.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i}of this.allViews){if(!r&&!co(i))continue;let o=r&&!this.zonelessEnabled?0:1;Sv(i,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>co(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let r=t;this._views.push(r),r.attachToAppRef(this)}detachView(t){let r=t;vo(this._views,r),r.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(i){this.internalErrorHandler(i)}this.components.push(t),this._injector.get(gc,[]).forEach(i=>i(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>vo(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new b(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function vo(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function ge(e,n,t,r){let i=R(),o=mr();if(Mt(i,o,n)){let s=fe(),a=uo();Gw(a,i,e,n,t,r)}return ge}function Ie(e,n,t,r,i,o,s,a){ui("NgControlFlow");let c=R(),l=fe(),d=Ht(l.consts,o);return Bf(c,l,e,n,t,r,i,d,256,s,a),Wf}function Wf(e,n,t,r,i,o,s,a){ui("NgControlFlow");let c=R(),l=fe(),d=Ht(l.consts,o);return Bf(c,l,e,n,t,r,i,d,512,s,a),Wf}function xe(e,n){ui("NgControlFlow");let t=R(),r=mr(),i=t[r]!==ct?t[r]:-1,o=i!==-1?_g(t,me+i):void 0,s=0;if(Mt(t,r,e)){let a=A(null);try{if(o!==void 0&&u0(o,s),e!==-1){let c=me+e,l=_g(t,c),d=gI(t[x],c),u=b0(l,d,t),p=Rf(t,d,n,{dehydratedView:u});Of(l,p,s,Qa(d,u))}}finally{A(a)}}else if(o!==void 0){let a=d0(o,s);a!==void 0&&(a[Ae]=n)}}function _g(e,n){return e[n]}function gI(e,n){return Da(e,n)}function ve(e,n,t){let r=R(),i=mr();if(Mt(r,i,n)){let o=fe(),s=uo();Dv(s,r,e,n,r[ie],t)}return ve}function Ku(e,n,t,r,i){Tf(n,e,t,i?"class":"style",r)}function _(e,n,t,r){let i=R(),o=i[x],s=e+me,a=o.firstCreatePass?kf(s,i,2,n,xf,wa(),t,r):o.data[s];if(nn(a)){let c=i[yt].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(Hv(l),()=>(Dg(e,n,i,a,r),_))}}return Dg(e,n,i,a,r),_}function Dg(e,n,t,r,i){if(Mf(r,t,e,n,iy),Jr(r)){let o=t[x];uc(o,t,r),af(o,r,t)}i!=null&&xo(t,r)}function C(){let e=fe(),n=ye(),t=Sf(n);return e.firstCreatePass&&Ff(e,t),Jd(t)&&eu(),Kd(),t.classesWithoutHost!=null&&cE(t)&&Ku(e,t,R(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&lE(t)&&Ku(e,t,R(),t.stylesWithoutHost,!1),C}function ae(e,n,t,r){return _(e,n,t,r),C(),ae}function Pe(e,n,t,r){let i=R(),o=i[x],s=e+me,a=o.firstCreatePass?A0(s,o,2,n,t,r):o.data[s];return Mf(a,i,e,n,iy),r!=null&&xo(i,a),Pe}function qe(){let e=ye(),n=Sf(e);return Jd(n)&&eu(),Kd(),qe}function Tt(e,n,t,r){return Pe(e,n,t,r),qe(),Tt}var iy=(e,n,t,r,i)=>(fo(!0),iv(n[ie],r,Vm()));function qf(e,n,t){let r=R(),i=r[x],o=e+me,s=i.firstCreatePass?kf(o,r,8,"ng-container",xf,wa(),n,t):i.data[o];if(Mf(s,r,e,"ng-container",vI),Jr(s)){let a=r[x];uc(a,r,s),af(a,s,r)}return t!=null&&xo(r,s),qf}function Zf(){let e=fe(),n=ye(),t=Sf(n);return e.firstCreatePass&&Ff(e,t),Zf}function pi(e,n,t){return qf(e,n,t),Zf(),pi}var vI=(e,n,t,r,i)=>(fo(!0),ew(n[ie],""));function Qf(){return R()}function wr(e,n,t){let r=R(),i=mr();if(Mt(r,i,n)){let o=fe(),s=uo();Cv(s,r,e,n,r[ie],t)}return wr}var Ao="en-US";var yI=Ao;function oy(e){typeof e=="string"&&(yI=e.toLowerCase().replace(/_/g,"-"))}function Ze(e,n,t){let r=R(),i=fe(),o=ye();return sy(i,r,r[ie],o,e,n,t),Ze}function vc(e,n,t){let r=R(),i=fe(),o=ye();return(o.type&3||t)&&Vv(o,i,r,t,r[ie],e,n,Ha(o,r,n)),vc}function sy(e,n,t,r,i,o,s){let a=!0,c=null;if((r.type&3||s)&&(c??=Ha(r,n,o),Vv(r,e,n,s,t,i,o,c)&&(a=!1)),a){let l=r.outputs?.[i],d=r.hostDirectiveOutputs?.[i];if(d&&d.length)for(let u=0;u<d.length;u+=2){let p=d[u],h=d[u+1];c??=Ha(r,n,o),gg(r,n,p,h,i,c)}if(l&&l.length)for(let u of l)c??=Ha(r,n,o),gg(r,n,u,i,i,c)}}function Le(e=1){return jm(e)}function bI(e,n){let t=null,r=dw(e);for(let i=0;i<n.length;i++){let o=n[i];if(o==="*"){t=i;continue}if(r===null?lv(e,o,!0):pw(r,o))return i}return t}function tt(e){let n=R()[Xe][Ge];if(!n.projection){let t=e?e.length:1,r=n.projection=fm(t,null),i=r.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=e?bI(o,e):0;s!==null&&(i[s]?i[s].projectionNext=o:r[s]=o,i[s]=o)}o=o.next}}}function le(e,n=0,t,r,i,o){let s=R(),a=fe(),c=r?e+1:null;c!==null&&Bf(s,a,c,r,i,o,null,t);let l=fi(a,me+e,16,null,t||null);l.projection===null&&(l.projection=n),ru();let u=!s[Qr]||Xd();s[Xe][Ge].projection[l.projection]===null&&c!==null?_I(s,a,c):u&&!sc(l)&&kw(a,s,l)}function _I(e,n,t){let r=me+t,i=n.data[r],o=e[r],s=Hu(o,i.tView.ssrId),a=Rf(e,i,void 0,{dehydratedView:s});Of(o,a,0,Qa(i,s))}function hi(e,n,t,r){return qv(e,n,t,r),hi}function dn(e,n,t){return Wv(e,n,t),dn}function ee(e){let n=R(),t=fe(),r=Sa();lo(r+1);let i=Lf(t,r);if(e.dirty&&Dm(n)===((i.metadata.flags&2)===2)){if(i.matches===null)e.reset([]);else{let o=Qv(n,r);e.reset(o,Ug),e.notifyOnChanges()}return!0}return!1}function te(){return Pf(R(),Sa())}function yc(e,n,t,r,i){return Kv(n,qv(e,t,r,i)),yc}function bc(e,n,t,r){return Kv(e,Wv(n,t,r)),bc}function _c(e=1){lo(Sa()+e)}function mi(e){let n=Tm();return zd(n,me+e)}function Pa(e,n){return e<<17|n<<2}function Dr(e){return e>>17&32767}function DI(e){return(e&2)==2}function CI(e,n){return e&131071|n<<17}function Xu(e){return e|2}function ci(e){return(e&131068)>>2}function _u(e,n){return e&-131069|n<<2}function EI(e){return(e&1)===1}function Ju(e){return e|1}function wI(e,n,t,r,i,o){let s=o?n.classBindings:n.styleBindings,a=Dr(s),c=ci(s);e[r]=t;let l=!1,d;if(Array.isArray(t)){let u=t;d=u[1],(d===null||qr(u,d)>0)&&(l=!0)}else d=t;if(i)if(c!==0){let p=Dr(e[a+1]);e[r+1]=Pa(p,a),p!==0&&(e[p+1]=_u(e[p+1],r)),e[a+1]=CI(e[a+1],r)}else e[r+1]=Pa(a,0),a!==0&&(e[a+1]=_u(e[a+1],r)),a=r;else e[r+1]=Pa(c,0),a===0?a=r:e[c+1]=_u(e[c+1],r),c=r;l&&(e[r+1]=Xu(e[r+1])),Cg(e,d,r,!0),Cg(e,d,r,!1),II(n,d,e,r,o),s=Pa(a,c),o?n.classBindings=s:n.styleBindings=s}function II(e,n,t,r,i){let o=i?e.residualClasses:e.residualStyles;o!=null&&typeof n=="string"&&qr(o,n)>=0&&(t[r+1]=Ju(t[r+1]))}function Cg(e,n,t,r){let i=e[t+1],o=n===null,s=r?Dr(i):ci(i),a=!1;for(;s!==0&&(a===!1||o);){let c=e[s],l=e[s+1];xI(c,n)&&(a=!0,e[s+1]=r?Ju(l):Xu(l)),s=r?Dr(l):ci(l)}a&&(e[t+1]=r?Xu(i):Ju(i))}function xI(e,n){return e===null||n==null||(Array.isArray(e)?e[1]:e)===n?!0:Array.isArray(e)&&typeof n=="string"?qr(e,n)>=0:!1}var wt={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function MI(e){return e.substring(wt.key,wt.keyEnd)}function SI(e){return TI(e),ay(e,cy(e,0,wt.textEnd))}function ay(e,n){let t=wt.textEnd;return t===n?-1:(n=wt.keyEnd=AI(e,wt.key=n,t),cy(e,n,t))}function TI(e){wt.key=0,wt.keyEnd=0,wt.value=0,wt.valueEnd=0,wt.textEnd=e.length}function cy(e,n,t){for(;n<t&&e.charCodeAt(n)<=32;)n++;return n}function AI(e,n,t){for(;n<t&&e.charCodeAt(n)>32;)n++;return n}function gi(e,n,t){return ly(e,n,t,!1),gi}function se(e,n){return ly(e,n,null,!0),se}function un(e){NI(jI,RI,e,!0)}function RI(e,n){for(let t=SI(n);t>=0;t=ay(n,t))ya(e,MI(n),!0)}function ly(e,n,t,r){let i=R(),o=fe(),s=su(2);if(o.firstUpdatePass&&uy(o,e,s,r),n!==ct&&Mt(i,s,n)){let a=o.data[Tn()];fy(o,a,i,i[ie],e,i[s+1]=BI(n,t),r,s)}}function NI(e,n,t,r){let i=fe(),o=su(2);i.firstUpdatePass&&uy(i,null,o,r);let s=R();if(t!==ct&&Mt(s,o,t)){let a=i.data[Tn()];if(py(a,r)&&!dy(i,o)){let c=r?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=fa(c,t||"")),Ku(i,a,s,t,r)}else VI(i,a,s,s[ie],s[o+1],s[o+1]=LI(e,n,t),r,o)}}function dy(e,n){return n>=e.expandoStartIndex}function uy(e,n,t,r){let i=e.data;if(i[t+1]===null){let o=i[Tn()],s=dy(e,t);py(o,r)&&n===null&&!s&&(n=!1),n=OI(i,o,n,r),wI(i,o,n,t,s,r)}}function OI(e,n,t,r){let i=km(e),o=r?n.residualClasses:n.residualStyles;if(i===null)(r?n.classBindings:n.styleBindings)===0&&(t=Du(null,e,n,t,r),t=Co(t,n.attrs,r),o=null);else{let s=n.directiveStylingLast;if(s===-1||e[s]!==i)if(t=Du(i,e,n,t,r),o===null){let c=kI(e,n,r);c!==void 0&&Array.isArray(c)&&(c=Du(null,e,n,c[1],r),c=Co(c,n.attrs,r),FI(e,n,r,c))}else o=PI(e,n,r)}return o!==void 0&&(r?n.residualClasses=o:n.residualStyles=o),t}function kI(e,n,t){let r=t?n.classBindings:n.styleBindings;if(ci(r)!==0)return e[Dr(r)]}function FI(e,n,t,r){let i=t?n.classBindings:n.styleBindings;e[Dr(i)]=r}function PI(e,n,t){let r,i=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<i;o++){let s=e[o].hostAttrs;r=Co(r,s,t)}return Co(r,n.attrs,t)}function Du(e,n,t,r,i){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=n[a],r=Co(r,o.hostAttrs,i),o!==e);)a++;return e!==null&&(t.directiveStylingLast=a),r}function Co(e,n,t){let r=t?1:2,i=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?i=s:i===r&&(Array.isArray(e)||(e=e===void 0?[]:["",e]),ya(e,s,t?!0:n[++o]))}return e===void 0?null:e}function LI(e,n,t){if(t==null||t==="")return Oe;let r=[],i=zt(t);if(Array.isArray(i))for(let o=0;o<i.length;o++)e(r,i[o],!0);else if(i instanceof Set)for(let o of i)e(r,o,!0);else if(typeof i=="object")for(let o in i)i.hasOwnProperty(o)&&e(r,o,i[o]);else typeof i=="string"&&n(r,i);return r}function jI(e,n,t){let r=String(n);r!==""&&!r.includes(" ")&&ya(e,r,t)}function VI(e,n,t,r,i,o,s,a){i===ct&&(i=Oe);let c=0,l=0,d=0<i.length?i[0]:null,u=0<o.length?o[0]:null;for(;d!==null||u!==null;){let p=c<i.length?i[c+1]:void 0,h=l<o.length?o[l+1]:void 0,g=null,I;d===u?(c+=2,l+=2,p!==h&&(g=u,I=h)):u===null||d!==null&&d<u?(c+=2,g=d):(l+=2,g=u,I=h),g!==null&&fy(e,n,t,r,g,I,s,a),d=c<i.length?i[c]:null,u=l<o.length?o[l]:null}}function fy(e,n,t,r,i,o,s,a){if(!(n.type&3))return;let c=e.data,l=c[a+1],d=EI(l)?Eg(c,n,t,i,ci(l),s):void 0;if(!rc(d)){rc(o)||DI(l)&&(o=Eg(c,null,t,i,a,s));let u=$d(Tn(),t);Pw(r,s,u,i,o)}}function Eg(e,n,t,r,i,o){let s=n===null,a;for(;i>0;){let c=e[i],l=Array.isArray(c),d=l?c[1]:c,u=d===null,p=t[i+1];p===ct&&(p=u?Oe:void 0);let h=u?ba(p,r):d===r?p:void 0;if(l&&!rc(h)&&(h=ba(c,r)),rc(h)&&(a=h,s))return a;let g=e[i+1];i=s?Dr(g):ci(g)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=ba(c,r))}return a}function rc(e){return e!==void 0}function BI(e,n){return e==null||e===""||(typeof n=="string"?e=e+n:typeof e=="object"&&(e=ro(zt(e)))),e}function py(e,n){return(e.flags&(n?8:16))!==0}function q(e,n=""){let t=R(),r=fe(),i=e+me,o=r.firstCreatePass?fi(r,i,1,n,null):r.data[i],s=HI(r,t,o,n);t[i]=s,Ra()&&wf(r,t,s,o),ti(o,!1)}var HI=(e,n,t,r)=>(fo(!0),XE(n[ie],r));function UI(e,n,t,r=""){return Mt(e,mr(),t)?n+ma(t)+r:ct}function nt(e){return Zt("",e),nt}function Zt(e,n,t){let r=R(),i=UI(r,e,n,t);return i!==ct&&$I(r,Tn(),i),Zt}function $I(e,n,t){let r=$d(n,e);JE(e[ie],r,t)}function Dc(e,n,t){Uf(n)&&(n=n());let r=R(),i=mr();if(Mt(r,i,n)){let o=fe(),s=uo();Dv(s,r,e,n,r[ie],t)}return Dc}function Yf(e,n){let t=Uf(e);return t&&e.set(n),t}function Cc(e,n){let t=R(),r=fe(),i=ye();return sy(r,t,t[ie],i,e,n),Cc}function wg(e,n,t){let r=fe();r.firstCreatePass&&hy(n,r.data,r.blueprint,Bt(e),t)}function hy(e,n,t,r,i){if(e=Te(e),Array.isArray(e))for(let o=0;o<e.length;o++)hy(e[o],n,t,r,i);else{let o=fe(),s=R(),a=ye(),c=ir(e)?e:Te(e.provide),l=jd(e),d=a.providerIndexes&1048575,u=a.directiveStart,p=a.providerIndexes>>20;if(ir(e)||!e.multi){let h=new vr(l,i,oe,null),g=Eu(c,n,i?d:d+p,u);g===-1?(Iu(Wa(a,s),o,c),Cu(o,e,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,i&&(a.providerIndexes+=1048576),t.push(h),s.push(h)):(t[g]=h,s[g]=h)}else{let h=Eu(c,n,d+p,u),g=Eu(c,n,d,d+p),I=h>=0&&t[h],D=g>=0&&t[g];if(i&&!D||!i&&!I){Iu(Wa(a,s),o,c);let w=WI(i?GI:zI,t.length,i,r,l,e);!i&&D&&(t[g].providerFactory=w),Cu(o,e,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,i&&(a.providerIndexes+=1048576),t.push(w),s.push(w)}else{let w=my(t[i?g:h],l,!i&&r);Cu(o,e,h>-1?h:g,w)}!i&&r&&D&&t[g].componentProviders++}}}function Cu(e,n,t,r){let i=ir(n),o=vm(n);if(i||o){let c=(o?Te(n.useClass):n).prototype.ngOnDestroy;if(c){let l=e.destroyHooks||(e.destroyHooks=[]);if(!i&&n.multi){let d=l.indexOf(t);d===-1?l.push(t,[r,c]):l[d+1].push(r,c)}else l.push(t,c)}}}function my(e,n,t){return t&&e.componentProviders++,e.multi.push(n)-1}function Eu(e,n,t,r){for(let i=t;i<r;i++)if(n[i]===e)return i;return-1}function zI(e,n,t,r,i){return ef(this.multi,[])}function GI(e,n,t,r,i){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=yo(r,r[x],this.providerFactory.index,i);s=c.slice(0,a),ef(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],ef(o,s);return s}function ef(e,n){for(let t=0;t<e.length;t++){let r=e[t];n.push(r())}return n}function WI(e,n,t,r,i,o){let s=new vr(e,t,oe,null);return s.multi=[],s.index=n,s.componentProviders=0,my(s,i,r&&!t),s}function Qt(e,n){return t=>{t.providersResolver=(r,i)=>wg(r,i?i(e):e,!1),n&&(t.viewProvidersResolver=(r,i)=>wg(r,i?i(n):n,!0))}}function Kf(e,n,t,r,i,o,s){return ZI(R(),ou(),e,n,t,r,i,o)}function gy(e,n){let t=e[n];return t===ct?void 0:t}function qI(e,n,t,r,i,o){let s=n+t;return Mt(e,s,i)?jv(e,s+1,o?r.call(o,i):r(i)):gy(e,s+1)}function ZI(e,n,t,r,i,o,s,a,c){let l=n+t;return R0(e,l,i,o,s,a)?jv(e,l+4,c?r.call(c,i,o,s,a):r(i,o,s,a)):gy(e,l+4)}function Ec(e,n){let t=fe(),r,i=e+me;t.firstCreatePass?(r=QI(n,t.pipeRegistry),t.data[i]=r,r.onDestroy&&(t.destroyHooks??=[]).push(i,r.onDestroy)):r=t.data[i];let o=r.factory||(r.factory=bn(r.type,!0)),s,a=ze(oe);try{let c=Ga(!1),l=o();return Ga(c),Gd(t,R(),i,l),l}finally{ze(a)}}function QI(e,n){if(n)for(let t=n.length-1;t>=0;t--){let r=n[t];if(e===r.name)return r}}function wc(e,n,t){let r=e+me,i=R(),o=zd(i,r);return YI(i,r)?qI(i,ou(),n,o.transform,t,o):o.transform(t)}function YI(e,n){return e[x].data[n].pure}function Ro(e,n){return fc(e,n)}var ic=class{ngModuleFactory;componentFactories;constructor(n,t){this.ngModuleFactory=n,this.componentFactories=t}},Xf=(()=>{class e{compileModuleSync(t){return new nc(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let r=this.compileModuleSync(t),i=Td(t),o=av(i.declarations).reduce((s,a)=>{let c=en(a);return c&&s.push(new br(c)),s},[]);return new ic(r,o)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var vy=(()=>{class e{applicationErrorHandler=f(Ct);appRef=f(qt);taskService=f(rn);ngZone=f($);zonelessEnabled=f(ho);tracing=f(Gt,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new pe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(to):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(f(pu,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?$m:du;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(to+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function yy(){return[{provide:jt,useExisting:vy},{provide:$,useClass:no},{provide:ho,useValue:!0}]}function KI(){return typeof $localize<"u"&&$localize.locale||Ao}var Ic=new m("",{factory:()=>f(Ic,{optional:!0,skipSelf:!0})||KI()});function je(e){return em(e)}function At(e,n){return Hi(e,n?.equal)}var wy=Symbol("InputSignalNode#UNSET"),px=P(v({},Ui),{transformFn:void 0,applyValueToInputSignal(e,n){Pr(e,n)}});function Iy(e,n){let t=Object.create(px);t.value=e,t.transformFn=n?.transform;function r(){if(qn(t),t.value===wy){let i=null;throw new b(-950,i)}return t.value}return r[be]=t,r}var Oo=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>nf(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},xy=(()=>{let e=new m("");return e.__NG_ELEMENT_ID__=n=>{let t=ye();if(t===null)throw new b(-204,!1);if(t.type&2)return t.value;if(n&8)return null;throw new b(-204,!1)},e})();function by(e,n){return Iy(e,n)}function hx(e){return Iy(wy,e)}var My=(by.required=hx,by);function _y(e,n){return jf(n)}function mx(e,n){return Vf(n)}var ko=(_y.required=mx,_y);function Dy(e,n){return jf(n)}function gx(e,n){return Vf(n)}var Sy=(Dy.required=gx,Dy);var ep=new m(""),vx=new m("");function No(e){return!e.moduleRef}function yx(e){let n=No(e)?e.r3Injector:e.moduleRef.injector,t=n.get($);return t.run(()=>{No(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let r=n.get(Ct),i;if(t.runOutsideAngular(()=>{i=t.onError.subscribe({next:r})}),No(e)){let o=()=>n.destroy(),s=e.platformInjector.get(ep);s.add(o),n.onDestroy(()=>{i.unsubscribe(),s.delete(o)})}else{let o=()=>e.moduleRef.destroy(),s=e.platformInjector.get(ep);s.add(o),e.moduleRef.onDestroy(()=>{vo(e.allPlatformModules,e.moduleRef),i.unsubscribe(),s.delete(o)})}return _x(r,t,()=>{let o=n.get(rn),s=o.add(),a=n.get(Gf);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(Ic,Ao);if(oy(c||Ao),!n.get(vx,!0))return No(e)?n.get(qt):(e.allPlatformModules.push(e.moduleRef),e.moduleRef);if(No(e)){let d=n.get(qt);return e.rootComponent!==void 0&&d.bootstrap(e.rootComponent),d}else return bx?.(e.moduleRef,e.allPlatformModules),e.moduleRef}).finally(()=>{o.remove(s)})})})}var bx;function _x(e,n,t){try{let r=t();return kn(r)?r.catch(i=>{throw n.runOutsideAngular(()=>e(i)),i}):r}catch(r){throw n.runOutsideAngular(()=>e(r)),r}}var xc=null;function Dx(e=[],n){return he.create({name:n,providers:[{provide:so,useValue:"platform"},{provide:ep,useValue:new Set([()=>xc=null])},...e]})}function Cx(e=[]){if(xc)return xc;let n=Dx(e);return xc=n,ny(),Ex(n),n}function Ex(e){let n=e.get(oc,null);we(e,()=>{n?.forEach(t=>t())})}var wx=1e4;var g$=wx-1e3;var Rt=(()=>{class e{static __NG_ELEMENT_ID__=Ix}return e})();function Ix(e){return xx(ye(),R(),(e&16)===16)}function xx(e,n,t){if(nn(e)&&!t){let r=at(e.index,n);return new Nn(r,r)}else if(e.type&175){let r=n[Xe];return new Nn(r,n)}return null}function Ty(e){let{rootComponent:n,appProviders:t,platformProviders:r,platformRef:i}=e;X(z.BootstrapApplicationStart);try{let o=i?.injector??Cx(r),s=[yy(),Gm,...t||[]],a=new Do({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return yx({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{X(z.BootstrapApplicationEnd)}}function Ve(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}function Fn(e,n=NaN){return!isNaN(parseFloat(e))&&!isNaN(Number(e))?Number(e):n}var Jf=Symbol("NOT_SET"),Ay=new Set,Mx=P(v({},Ui),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Jf,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(e){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Jf&&!kr(this))return this.signal;try{for(let i of this.cleanup??Ay)i()}finally{this.cleanup?.clear()}let n=[];e!==void 0&&n.push(e),n.push(this.registerCleanupFn);let t=vn(this),r;try{r=this.userFn.apply(null,n)}finally{Zn(this,t)}return(this.value===Jf||!this.equal(this.value,r))&&(this.value=r,this.version++),this.signal}}),tp=class extends Za{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,t,r,i,o,s=null){super(n,[void 0,void 0,void 0,void 0],r,!1,o.get(Dt),s),this.scheduler=i;for(let a of Df){let c=t[a];if(c===void 0)continue;let l=Object.create(Mx);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(qn(l),l.value),l.signal[be]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let t of n.cleanup??Ay)t()}finally{yn(n)}}};function Ry(e,n){let t=n?.injector??f(he),r=t.get(jt),i=t.get(_f),o=t.get(Gt,null,{optional:!0});i.impl??=t.get(pv);let s=e;typeof s=="function"&&(s={mixedReadWrite:e});let a=t.get(po,null,{optional:!0}),c=new tp(i.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,r,t,o?.snapshot(null));return i.impl.register(c),c}function Ny(e,n){let t=en(e),r=n.elementInjector||Zr();return new br(t).create(r,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var Oy=null;function dt(){return Oy}function np(e){Oy??=e}var Fo=class{},vi=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>f(ky),providedIn:"platform"})}return e})();var ky=(()=>{class e extends vi{_location;_history;_doc=f(U);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return dt().getBaseHref(this._doc)}onPopState(t){let r=dt().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",t,!1),()=>r.removeEventListener("popstate",t)}onHashChange(t){let r=dt().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",t,!1),()=>r.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,r,i){this._history.pushState(t,r,i)}replaceState(t,r,i){this._history.replaceState(t,r,i)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>new e,providedIn:"platform"})}return e})();function Ly(e,n){return e?n?e.endsWith("/")?n.startsWith("/")?e+n.slice(1):e+n:n.startsWith("/")?e+n:`${e}/${n}`:e:n}function Fy(e){let n=e.search(/#|\?|$/);return e[n-1]==="/"?e.slice(0,n-1)+e.slice(n):e}function Pn(e){return e&&e[0]!=="?"?`?${e}`:e}var Tc=(()=>{class e{historyGo(t){throw new Error("")}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>f(Tx),providedIn:"root"})}return e})(),Sx=new m(""),Tx=(()=>{class e extends Tc{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,r){super(),this._platformLocation=t,this._baseHref=r??this._platformLocation.getBaseHrefFromDOM()??f(U).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return Ly(this._baseHref,t)}path(t=!1){let r=this._platformLocation.pathname+Pn(this._platformLocation.search),i=this._platformLocation.hash;return i&&t?`${r}${i}`:r}pushState(t,r,i,o){let s=this.prepareExternalUrl(i+Pn(o));this._platformLocation.pushState(t,r,s)}replaceState(t,r,i,o){let s=this.prepareExternalUrl(i+Pn(o));this._platformLocation.replaceState(t,r,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(r){return new(r||e)(E(vi),E(Sx,8))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var yi=(()=>{class e{_subject=new H;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let r=this._locationStrategy.getBaseHref();this._basePath=Nx(Fy(Py(r))),this._locationStrategy.onPopState(i=>{this._subject.next({url:this.path(!0),pop:!0,state:i.state,type:i.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,r=""){return this.path()==this.normalize(t+Pn(r))}normalize(t){return e.stripTrailingSlash(Rx(this._basePath,Py(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,r="",i=null){this._locationStrategy.pushState(i,"",t,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Pn(r)),i)}replaceState(t,r="",i=null){this._locationStrategy.replaceState(i,"",t,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+Pn(r)),i)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(r=>{this._notifyUrlChangeListeners(r.url,r.state)}),()=>{let r=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(r,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",r){this._urlChangeListeners.forEach(i=>i(t,r))}subscribe(t,r,i){return this._subject.subscribe({next:t,error:r??void 0,complete:i??void 0})}static normalizeQueryParams=Pn;static joinWithSlash=Ly;static stripTrailingSlash=Fy;static \u0275fac=function(r){return new(r||e)(E(Tc))};static \u0275prov=y({token:e,factory:()=>Ax(),providedIn:"root"})}return e})();function Ax(){return new yi(E(Tc))}function Rx(e,n){if(!e||!n.startsWith(e))return n;let t=n.substring(e.length);return t===""||["/",";","?","#"].includes(t[0])?t:n}function Py(e){return e.replace(/\/index.html$/,"")}function Nx(e){if(new RegExp("^(https?:)?//").test(e)){let[,t]=e.split(/\/\/[^\/]+/);return t}return e}var rp=/\s+/,jy=[],ip=(()=>{class e{_ngEl;_renderer;initialClasses=jy;rawClass;stateMap=new Map;constructor(t,r){this._ngEl=t,this._renderer=r}set klass(t){this.initialClasses=t!=null?t.trim().split(rp):jy}set ngClass(t){this.rawClass=typeof t=="string"?t.trim().split(rp):t}ngDoCheck(){for(let r of this.initialClasses)this._updateState(r,!0);let t=this.rawClass;if(Array.isArray(t)||t instanceof Set)for(let r of t)this._updateState(r,!0);else if(t!=null)for(let r of Object.keys(t))this._updateState(r,!!t[r]);this._applyStateDiff()}_updateState(t,r){let i=this.stateMap.get(t);i!==void 0?(i.enabled!==r&&(i.changed=!0,i.enabled=r),i.touched=!0):this.stateMap.set(t,{enabled:r,changed:!0,touched:!0})}_applyStateDiff(){for(let t of this.stateMap){let r=t[0],i=t[1];i.changed?(this._toggleClass(r,i.enabled),i.changed=!1):i.touched||(i.enabled&&this._toggleClass(r,!1),this.stateMap.delete(r)),i.touched=!1}}_toggleClass(t,r){t=t.trim(),t.length>0&&t.split(rp).forEach(i=>{r?this._renderer.addClass(this._ngEl.nativeElement,i):this._renderer.removeClass(this._ngEl.nativeElement,i)})}static \u0275fac=function(r){return new(r||e)(oe(J),oe(St))};static \u0275dir=Q({type:e,selectors:[["","ngClass",""]],inputs:{klass:[0,"class","klass"],ngClass:"ngClass"}})}return e})();var op=(()=>{class e{_viewContainer;_context=new Ac;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(t,r){this._viewContainer=t,this._thenTemplateRef=r}set ngIf(t){this._context.$implicit=this._context.ngIf=t,this._updateView()}set ngIfThen(t){Vy(t,!1),this._thenTemplateRef=t,this._thenViewRef=null,this._updateView()}set ngIfElse(t){Vy(t,!1),this._elseTemplateRef=t,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(t,r){return!0}static \u0275fac=function(r){return new(r||e)(oe(ln),oe(yr))};static \u0275dir=Q({type:e,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return e})(),Ac=class{$implicit=null;ngIf=null};function Vy(e,n){if(e&&!e.createEmbeddedView)throw new b(2020,!1)}var Po=(()=>{class e{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=f(he);constructor(t){this._viewContainerRef=t}ngOnChanges(t){if(this._shouldRecreateView(t)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let i=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,i,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(t){return!!t.ngTemplateOutlet||!!t.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(t,r,i)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,i):!1,get:(t,r,i)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,i)}})}static \u0275fac=function(r){return new(r||e)(oe(ln))};static \u0275dir=Q({type:e,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[sn]})}return e})();function Ox(e,n){return new b(2100,!1)}var kx=/(?:[0-9A-Za-z\xAA\xB5\xBA\xC0-\xD6\xD8-\xF6\xF8-\u02C1\u02C6-\u02D1\u02E0-\u02E4\u02EC\u02EE\u0370-\u0374\u0376\u0377\u037A-\u037D\u037F\u0386\u0388-\u038A\u038C\u038E-\u03A1\u03A3-\u03F5\u03F7-\u0481\u048A-\u052F\u0531-\u0556\u0559\u0560-\u0588\u05D0-\u05EA\u05EF-\u05F2\u0620-\u064A\u066E\u066F\u0671-\u06D3\u06D5\u06E5\u06E6\u06EE\u06EF\u06FA-\u06FC\u06FF\u0710\u0712-\u072F\u074D-\u07A5\u07B1\u07CA-\u07EA\u07F4\u07F5\u07FA\u0800-\u0815\u081A\u0824\u0828\u0840-\u0858\u0860-\u086A\u0870-\u0887\u0889-\u088E\u08A0-\u08C9\u0904-\u0939\u093D\u0950\u0958-\u0961\u0971-\u0980\u0985-\u098C\u098F\u0990\u0993-\u09A8\u09AA-\u09B0\u09B2\u09B6-\u09B9\u09BD\u09CE\u09DC\u09DD\u09DF-\u09E1\u09F0\u09F1\u09FC\u0A05-\u0A0A\u0A0F\u0A10\u0A13-\u0A28\u0A2A-\u0A30\u0A32\u0A33\u0A35\u0A36\u0A38\u0A39\u0A59-\u0A5C\u0A5E\u0A72-\u0A74\u0A85-\u0A8D\u0A8F-\u0A91\u0A93-\u0AA8\u0AAA-\u0AB0\u0AB2\u0AB3\u0AB5-\u0AB9\u0ABD\u0AD0\u0AE0\u0AE1\u0AF9\u0B05-\u0B0C\u0B0F\u0B10\u0B13-\u0B28\u0B2A-\u0B30\u0B32\u0B33\u0B35-\u0B39\u0B3D\u0B5C\u0B5D\u0B5F-\u0B61\u0B71\u0B83\u0B85-\u0B8A\u0B8E-\u0B90\u0B92-\u0B95\u0B99\u0B9A\u0B9C\u0B9E\u0B9F\u0BA3\u0BA4\u0BA8-\u0BAA\u0BAE-\u0BB9\u0BD0\u0C05-\u0C0C\u0C0E-\u0C10\u0C12-\u0C28\u0C2A-\u0C39\u0C3D\u0C58-\u0C5A\u0C5D\u0C60\u0C61\u0C80\u0C85-\u0C8C\u0C8E-\u0C90\u0C92-\u0CA8\u0CAA-\u0CB3\u0CB5-\u0CB9\u0CBD\u0CDD\u0CDE\u0CE0\u0CE1\u0CF1\u0CF2\u0D04-\u0D0C\u0D0E-\u0D10\u0D12-\u0D3A\u0D3D\u0D4E\u0D54-\u0D56\u0D5F-\u0D61\u0D7A-\u0D7F\u0D85-\u0D96\u0D9A-\u0DB1\u0DB3-\u0DBB\u0DBD\u0DC0-\u0DC6\u0E01-\u0E30\u0E32\u0E33\u0E40-\u0E46\u0E81\u0E82\u0E84\u0E86-\u0E8A\u0E8C-\u0EA3\u0EA5\u0EA7-\u0EB0\u0EB2\u0EB3\u0EBD\u0EC0-\u0EC4\u0EC6\u0EDC-\u0EDF\u0F00\u0F40-\u0F47\u0F49-\u0F6C\u0F88-\u0F8C\u1000-\u102A\u103F\u1050-\u1055\u105A-\u105D\u1061\u1065\u1066\u106E-\u1070\u1075-\u1081\u108E\u10A0-\u10C5\u10C7\u10CD\u10D0-\u10FA\u10FC-\u1248\u124A-\u124D\u1250-\u1256\u1258\u125A-\u125D\u1260-\u1288\u128A-\u128D\u1290-\u12B0\u12B2-\u12B5\u12B8-\u12BE\u12C0\u12C2-\u12C5\u12C8-\u12D6\u12D8-\u1310\u1312-\u1315\u1318-\u135A\u1380-\u138F\u13A0-\u13F5\u13F8-\u13FD\u1401-\u166C\u166F-\u167F\u1681-\u169A\u16A0-\u16EA\u16F1-\u16F8\u1700-\u1711\u171F-\u1731\u1740-\u1751\u1760-\u176C\u176E-\u1770\u1780-\u17B3\u17D7\u17DC\u1820-\u1878\u1880-\u1884\u1887-\u18A8\u18AA\u18B0-\u18F5\u1900-\u191E\u1950-\u196D\u1970-\u1974\u1980-\u19AB\u19B0-\u19C9\u1A00-\u1A16\u1A20-\u1A54\u1AA7\u1B05-\u1B33\u1B45-\u1B4C\u1B83-\u1BA0\u1BAE\u1BAF\u1BBA-\u1BE5\u1C00-\u1C23\u1C4D-\u1C4F\u1C5A-\u1C7D\u1C80-\u1C88\u1C90-\u1CBA\u1CBD-\u1CBF\u1CE9-\u1CEC\u1CEE-\u1CF3\u1CF5\u1CF6\u1CFA\u1D00-\u1DBF\u1E00-\u1F15\u1F18-\u1F1D\u1F20-\u1F45\u1F48-\u1F4D\u1F50-\u1F57\u1F59\u1F5B\u1F5D\u1F5F-\u1F7D\u1F80-\u1FB4\u1FB6-\u1FBC\u1FBE\u1FC2-\u1FC4\u1FC6-\u1FCC\u1FD0-\u1FD3\u1FD6-\u1FDB\u1FE0-\u1FEC\u1FF2-\u1FF4\u1FF6-\u1FFC\u2071\u207F\u2090-\u209C\u2102\u2107\u210A-\u2113\u2115\u2119-\u211D\u2124\u2126\u2128\u212A-\u212D\u212F-\u2139\u213C-\u213F\u2145-\u2149\u214E\u2183\u2184\u2C00-\u2CE4\u2CEB-\u2CEE\u2CF2\u2CF3\u2D00-\u2D25\u2D27\u2D2D\u2D30-\u2D67\u2D6F\u2D80-\u2D96\u2DA0-\u2DA6\u2DA8-\u2DAE\u2DB0-\u2DB6\u2DB8-\u2DBE\u2DC0-\u2DC6\u2DC8-\u2DCE\u2DD0-\u2DD6\u2DD8-\u2DDE\u2E2F\u3005\u3006\u3031-\u3035\u303B\u303C\u3041-\u3096\u309D-\u309F\u30A1-\u30FA\u30FC-\u30FF\u3105-\u312F\u3131-\u318E\u31A0-\u31BF\u31F0-\u31FF\u3400-\u4DBF\u4E00-\uA48C\uA4D0-\uA4FD\uA500-\uA60C\uA610-\uA61F\uA62A\uA62B\uA640-\uA66E\uA67F-\uA69D\uA6A0-\uA6E5\uA717-\uA71F\uA722-\uA788\uA78B-\uA7CA\uA7D0\uA7D1\uA7D3\uA7D5-\uA7D9\uA7F2-\uA801\uA803-\uA805\uA807-\uA80A\uA80C-\uA822\uA840-\uA873\uA882-\uA8B3\uA8F2-\uA8F7\uA8FB\uA8FD\uA8FE\uA90A-\uA925\uA930-\uA946\uA960-\uA97C\uA984-\uA9B2\uA9CF\uA9E0-\uA9E4\uA9E6-\uA9EF\uA9FA-\uA9FE\uAA00-\uAA28\uAA40-\uAA42\uAA44-\uAA4B\uAA60-\uAA76\uAA7A\uAA7E-\uAAAF\uAAB1\uAAB5\uAAB6\uAAB9-\uAABD\uAAC0\uAAC2\uAADB-\uAADD\uAAE0-\uAAEA\uAAF2-\uAAF4\uAB01-\uAB06\uAB09-\uAB0E\uAB11-\uAB16\uAB20-\uAB26\uAB28-\uAB2E\uAB30-\uAB5A\uAB5C-\uAB69\uAB70-\uABE2\uAC00-\uD7A3\uD7B0-\uD7C6\uD7CB-\uD7FB\uF900-\uFA6D\uFA70-\uFAD9\uFB00-\uFB06\uFB13-\uFB17\uFB1D\uFB1F-\uFB28\uFB2A-\uFB36\uFB38-\uFB3C\uFB3E\uFB40\uFB41\uFB43\uFB44\uFB46-\uFBB1\uFBD3-\uFD3D\uFD50-\uFD8F\uFD92-\uFDC7\uFDF0-\uFDFB\uFE70-\uFE74\uFE76-\uFEFC\uFF21-\uFF3A\uFF41-\uFF5A\uFF66-\uFFBE\uFFC2-\uFFC7\uFFCA-\uFFCF\uFFD2-\uFFD7\uFFDA-\uFFDC]|\uD800[\uDC00-\uDC0B\uDC0D-\uDC26\uDC28-\uDC3A\uDC3C\uDC3D\uDC3F-\uDC4D\uDC50-\uDC5D\uDC80-\uDCFA\uDE80-\uDE9C\uDEA0-\uDED0\uDF00-\uDF1F\uDF2D-\uDF40\uDF42-\uDF49\uDF50-\uDF75\uDF80-\uDF9D\uDFA0-\uDFC3\uDFC8-\uDFCF]|\uD801[\uDC00-\uDC9D\uDCB0-\uDCD3\uDCD8-\uDCFB\uDD00-\uDD27\uDD30-\uDD63\uDD70-\uDD7A\uDD7C-\uDD8A\uDD8C-\uDD92\uDD94\uDD95\uDD97-\uDDA1\uDDA3-\uDDB1\uDDB3-\uDDB9\uDDBB\uDDBC\uDE00-\uDF36\uDF40-\uDF55\uDF60-\uDF67\uDF80-\uDF85\uDF87-\uDFB0\uDFB2-\uDFBA]|\uD802[\uDC00-\uDC05\uDC08\uDC0A-\uDC35\uDC37\uDC38\uDC3C\uDC3F-\uDC55\uDC60-\uDC76\uDC80-\uDC9E\uDCE0-\uDCF2\uDCF4\uDCF5\uDD00-\uDD15\uDD20-\uDD39\uDD80-\uDDB7\uDDBE\uDDBF\uDE00\uDE10-\uDE13\uDE15-\uDE17\uDE19-\uDE35\uDE60-\uDE7C\uDE80-\uDE9C\uDEC0-\uDEC7\uDEC9-\uDEE4\uDF00-\uDF35\uDF40-\uDF55\uDF60-\uDF72\uDF80-\uDF91]|\uD803[\uDC00-\uDC48\uDC80-\uDCB2\uDCC0-\uDCF2\uDD00-\uDD23\uDE80-\uDEA9\uDEB0\uDEB1\uDF00-\uDF1C\uDF27\uDF30-\uDF45\uDF70-\uDF81\uDFB0-\uDFC4\uDFE0-\uDFF6]|\uD804[\uDC03-\uDC37\uDC71\uDC72\uDC75\uDC83-\uDCAF\uDCD0-\uDCE8\uDD03-\uDD26\uDD44\uDD47\uDD50-\uDD72\uDD76\uDD83-\uDDB2\uDDC1-\uDDC4\uDDDA\uDDDC\uDE00-\uDE11\uDE13-\uDE2B\uDE80-\uDE86\uDE88\uDE8A-\uDE8D\uDE8F-\uDE9D\uDE9F-\uDEA8\uDEB0-\uDEDE\uDF05-\uDF0C\uDF0F\uDF10\uDF13-\uDF28\uDF2A-\uDF30\uDF32\uDF33\uDF35-\uDF39\uDF3D\uDF50\uDF5D-\uDF61]|\uD805[\uDC00-\uDC34\uDC47-\uDC4A\uDC5F-\uDC61\uDC80-\uDCAF\uDCC4\uDCC5\uDCC7\uDD80-\uDDAE\uDDD8-\uDDDB\uDE00-\uDE2F\uDE44\uDE80-\uDEAA\uDEB8\uDF00-\uDF1A\uDF40-\uDF46]|\uD806[\uDC00-\uDC2B\uDCA0-\uDCDF\uDCFF-\uDD06\uDD09\uDD0C-\uDD13\uDD15\uDD16\uDD18-\uDD2F\uDD3F\uDD41\uDDA0-\uDDA7\uDDAA-\uDDD0\uDDE1\uDDE3\uDE00\uDE0B-\uDE32\uDE3A\uDE50\uDE5C-\uDE89\uDE9D\uDEB0-\uDEF8]|\uD807[\uDC00-\uDC08\uDC0A-\uDC2E\uDC40\uDC72-\uDC8F\uDD00-\uDD06\uDD08\uDD09\uDD0B-\uDD30\uDD46\uDD60-\uDD65\uDD67\uDD68\uDD6A-\uDD89\uDD98\uDEE0-\uDEF2\uDFB0]|\uD808[\uDC00-\uDF99]|\uD809[\uDC80-\uDD43]|\uD80B[\uDF90-\uDFF0]|[\uD80C\uD81C-\uD820\uD822\uD840-\uD868\uD86A-\uD86C\uD86F-\uD872\uD874-\uD879\uD880-\uD883][\uDC00-\uDFFF]|\uD80D[\uDC00-\uDC2E]|\uD811[\uDC00-\uDE46]|\uD81A[\uDC00-\uDE38\uDE40-\uDE5E\uDE70-\uDEBE\uDED0-\uDEED\uDF00-\uDF2F\uDF40-\uDF43\uDF63-\uDF77\uDF7D-\uDF8F]|\uD81B[\uDE40-\uDE7F\uDF00-\uDF4A\uDF50\uDF93-\uDF9F\uDFE0\uDFE1\uDFE3]|\uD821[\uDC00-\uDFF7]|\uD823[\uDC00-\uDCD5\uDD00-\uDD08]|\uD82B[\uDFF0-\uDFF3\uDFF5-\uDFFB\uDFFD\uDFFE]|\uD82C[\uDC00-\uDD22\uDD50-\uDD52\uDD64-\uDD67\uDD70-\uDEFB]|\uD82F[\uDC00-\uDC6A\uDC70-\uDC7C\uDC80-\uDC88\uDC90-\uDC99]|\uD835[\uDC00-\uDC54\uDC56-\uDC9C\uDC9E\uDC9F\uDCA2\uDCA5\uDCA6\uDCA9-\uDCAC\uDCAE-\uDCB9\uDCBB\uDCBD-\uDCC3\uDCC5-\uDD05\uDD07-\uDD0A\uDD0D-\uDD14\uDD16-\uDD1C\uDD1E-\uDD39\uDD3B-\uDD3E\uDD40-\uDD44\uDD46\uDD4A-\uDD50\uDD52-\uDEA5\uDEA8-\uDEC0\uDEC2-\uDEDA\uDEDC-\uDEFA\uDEFC-\uDF14\uDF16-\uDF34\uDF36-\uDF4E\uDF50-\uDF6E\uDF70-\uDF88\uDF8A-\uDFA8\uDFAA-\uDFC2\uDFC4-\uDFCB]|\uD837[\uDF00-\uDF1E]|\uD838[\uDD00-\uDD2C\uDD37-\uDD3D\uDD4E\uDE90-\uDEAD\uDEC0-\uDEEB]|\uD839[\uDFE0-\uDFE6\uDFE8-\uDFEB\uDFED\uDFEE\uDFF0-\uDFFE]|\uD83A[\uDC00-\uDCC4\uDD00-\uDD43\uDD4B]|\uD83B[\uDE00-\uDE03\uDE05-\uDE1F\uDE21\uDE22\uDE24\uDE27\uDE29-\uDE32\uDE34-\uDE37\uDE39\uDE3B\uDE42\uDE47\uDE49\uDE4B\uDE4D-\uDE4F\uDE51\uDE52\uDE54\uDE57\uDE59\uDE5B\uDE5D\uDE5F\uDE61\uDE62\uDE64\uDE67-\uDE6A\uDE6C-\uDE72\uDE74-\uDE77\uDE79-\uDE7C\uDE7E\uDE80-\uDE89\uDE8B-\uDE9B\uDEA1-\uDEA3\uDEA5-\uDEA9\uDEAB-\uDEBB]|\uD869[\uDC00-\uDEDF\uDF00-\uDFFF]|\uD86D[\uDC00-\uDF38\uDF40-\uDFFF]|\uD86E[\uDC00-\uDC1D\uDC20-\uDFFF]|\uD873[\uDC00-\uDEA1\uDEB0-\uDFFF]|\uD87A[\uDC00-\uDFE0]|\uD87E[\uDC00-\uDE1D]|\uD884[\uDC00-\uDF4A])\S*/g,sp=(()=>{class e{transform(t){return t==null?null:(Fx(e,t),t.replace(kx,r=>r[0].toUpperCase()+r.slice(1).toLowerCase()))}static \u0275fac=function(r){return new(r||e)};static \u0275pipe=To({name:"titlecase",type:e,pure:!0})}return e})();function Fx(e,n){if(typeof n!="string")throw Ox(e,n)}var Rc=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({})}return e})();function Lo(e,n){n=encodeURIComponent(n);for(let t of e.split(";")){let r=t.indexOf("="),[i,o]=r==-1?[t,""]:[t.slice(0,r),t.slice(r+1)];if(i.trim()===n)return decodeURIComponent(o)}return null}var Ir=class{};var ap="browser";function By(e){return e===ap}var jo=class{_doc;constructor(n){this._doc=n}manager},Nc=(()=>{class e extends jo{constructor(t){super(t)}supports(t){return!0}addEventListener(t,r,i,o){return t.addEventListener(r,i,o),()=>this.removeEventListener(t,r,i,o)}removeEventListener(t,r,i,o){return t.removeEventListener(r,i,o)}static \u0275fac=function(r){return new(r||e)(E(U))};static \u0275prov=y({token:e,factory:e.\u0275fac})}return e})(),Fc=new m(""),up=(()=>{class e{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,r){this._zone=r,t.forEach(s=>{s.manager=this});let i=t.filter(s=>!(s instanceof Nc));this._plugins=i.slice().reverse();let o=t.find(s=>s instanceof Nc);o&&this._plugins.push(o)}addEventListener(t,r,i,o){return this._findPluginFor(r).addEventListener(t,r,i,o)}getZone(){return this._zone}_findPluginFor(t){let r=this._eventNameToPlugin.get(t);if(r)return r;if(r=this._plugins.find(o=>o.supports(t)),!r)throw new b(5101,!1);return this._eventNameToPlugin.set(t,r),r}static \u0275fac=function(r){return new(r||e)(E(Fc),E($))};static \u0275prov=y({token:e,factory:e.\u0275fac})}return e})(),cp="ng-app-id";function Hy(e){for(let n of e)n.remove()}function Uy(e,n){let t=n.createElement("style");return t.textContent=e,t}function jx(e,n,t,r){let i=e.head?.querySelectorAll(`style[${cp}="${n}"],link[${cp}="${n}"]`);if(i)for(let o of i)o.removeAttribute(cp),o instanceof HTMLLinkElement?r.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]})}function dp(e,n){let t=n.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",e),t}var fp=(()=>{class e{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,r,i,o={}){this.doc=t,this.appId=r,this.nonce=i,jx(t,r,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,r){for(let i of t)this.addUsage(i,this.inline,Uy);r?.forEach(i=>this.addUsage(i,this.external,dp))}removeStyles(t,r){for(let i of t)this.removeUsage(i,this.inline);r?.forEach(i=>this.removeUsage(i,this.external))}addUsage(t,r,i){let o=r.get(t);o?o.usage++:r.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,i(t,this.doc)))})}removeUsage(t,r){let i=r.get(t);i&&(i.usage--,i.usage<=0&&(Hy(i.elements),r.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])Hy(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[r,{elements:i}]of this.inline)i.push(this.addElement(t,Uy(r,this.doc)));for(let[r,{elements:i}]of this.external)i.push(this.addElement(t,dp(r,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,r){return this.nonce&&r.setAttribute("nonce",this.nonce),t.appendChild(r)}static \u0275fac=function(r){return new(r||e)(E(U),E(di),E(Er,8),E(Cr))};static \u0275prov=y({token:e,factory:e.\u0275fac})}return e})(),lp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},pp=/%COMP%/g;var zy="%COMP%",Vx=`_nghost-${zy}`,Bx=`_ngcontent-${zy}`,Hx=!0,Ux=new m("",{factory:()=>Hx});function $x(e){return Bx.replace(pp,e)}function zx(e){return Vx.replace(pp,e)}function Gy(e,n){return n.map(t=>t.replace(pp,e))}var hp=(()=>{class e{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,r,i,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=r,this.appId=i,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Vo(t,s,a,this.tracingService)}createRenderer(t,r){if(!t||!r)return this.defaultRenderer;let i=this.getOrCreateRenderer(t,r);return i instanceof kc?i.applyToHost(t):i instanceof Bo&&i.applyStyles(),i}getOrCreateRenderer(t,r){let i=this.rendererByCompId,o=i.get(r.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,u=this.tracingService;switch(r.encapsulation){case It.Emulated:o=new kc(c,l,r,this.appId,d,s,a,u);break;case It.ShadowDom:return new Oc(c,t,r,s,a,this.nonce,u,l);case It.ExperimentalIsolatedShadowDom:return new Oc(c,t,r,s,a,this.nonce,u);default:o=new Bo(c,l,r,d,s,a,u);break}i.set(r.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(r){return new(r||e)(E(up),E(fp),E(di),E(Ux),E(U),E($),E(Er),E(Gt,8))};static \u0275prov=y({token:e,factory:e.\u0275fac})}return e})(),Vo=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,t,r,i){this.eventManager=n,this.doc=t,this.ngZone=r,this.tracingService=i}destroy(){}destroyNode=null;createElement(n,t){return t?this.doc.createElementNS(lp[t]||t,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,t){($y(n)?n.content:n).appendChild(t)}insertBefore(n,t,r){n&&($y(n)?n.content:n).insertBefore(t,r)}removeChild(n,t){t.remove()}selectRootElement(n,t){let r=typeof n=="string"?this.doc.querySelector(n):n;if(!r)throw new b(-5104,!1);return t||(r.textContent=""),r}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,t,r,i){if(i){t=i+":"+t;let o=lp[i];o?n.setAttributeNS(o,t,r):n.setAttribute(t,r)}else n.setAttribute(t,r)}removeAttribute(n,t,r){if(r){let i=lp[r];i?n.removeAttributeNS(i,t):n.removeAttribute(`${r}:${t}`)}else n.removeAttribute(t)}addClass(n,t){n.classList.add(t)}removeClass(n,t){n.classList.remove(t)}setStyle(n,t,r,i){i&($t.DashCase|$t.Important)?n.style.setProperty(t,r,i&$t.Important?"important":""):n.style[t]=r}removeStyle(n,t,r){r&$t.DashCase?n.style.removeProperty(t):n.style[t]=""}setProperty(n,t,r){n!=null&&(n[t]=r)}setValue(n,t){n.nodeValue=t}listen(n,t,r,i){if(typeof n=="string"&&(n=dt().getGlobalEventTarget(this.doc,n),!n))throw new b(5102,!1);let o=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,t,o)),this.eventManager.addEventListener(n,t,o,i)}decoratePreventDefault(n){return t=>{if(t==="__ngUnwrap__")return n;n(t)===!1&&t.preventDefault()}}};function $y(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var Oc=class extends Vo{hostEl;sharedStylesHost;shadowRoot;constructor(n,t,r,i,o,s,a,c){super(n,i,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=r.styles;l=Gy(r.id,l);for(let u of l){let p=document.createElement("style");s&&p.setAttribute("nonce",s),p.textContent=u,this.shadowRoot.appendChild(p)}let d=r.getExternalStyles?.();if(d)for(let u of d){let p=dp(u,i);s&&p.setAttribute("nonce",s),this.shadowRoot.appendChild(p)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,t){return super.appendChild(this.nodeOrShadowRoot(n),t)}insertBefore(n,t,r){return super.insertBefore(this.nodeOrShadowRoot(n),t,r)}removeChild(n,t){return super.removeChild(null,t)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Bo=class extends Vo{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,t,r,i,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=i;let l=r.styles;this.styles=c?Gy(c,l):l,this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&ai.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},kc=class extends Bo{contentAttr;hostAttr;constructor(n,t,r,i,o,s,a,c){let l=i+"-"+r.id;super(n,t,r,o,s,a,c,l),this.contentAttr=$x(l),this.hostAttr=zx(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,t){let r=super.createElement(n,t);return super.setAttribute(r,this.contentAttr,""),r}};var Pc=class e extends Fo{supportsDOMEvents=!0;static makeCurrent(){np(new e)}onAndCancel(n,t,r,i){return n.addEventListener(t,r,i),()=>{n.removeEventListener(t,r,i)}}dispatchEvent(n,t){n.dispatchEvent(t)}remove(n){n.remove()}createElement(n,t){return t=t||this.getDefaultDocument(),t.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,t){return t==="window"?window:t==="document"?n:t==="body"?n.body:null}getBaseHref(n){let t=Gx();return t==null?null:Wx(t)}resetBaseElement(){Ho=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Lo(document.cookie,n)}},Ho=null;function Gx(){return Ho=Ho||document.head.querySelector("base"),Ho?Ho.getAttribute("href"):null}function Wx(e){return new URL(e,document.baseURI).pathname}var qx=(()=>{class e{build(){return new XMLHttpRequest}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac})}return e})(),Wy=["alt","control","meta","shift"],Zx={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},Qx={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},qy=(()=>{class e extends jo{constructor(t){super(t)}supports(t){return e.parseEventName(t)!=null}addEventListener(t,r,i,o){let s=e.parseEventName(r),a=e.eventCallback(s.fullKey,i,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>dt().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let r=t.toLowerCase().split("."),i=r.shift();if(r.length===0||!(i==="keydown"||i==="keyup"))return null;let o=e._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),Wy.forEach(l=>{let d=r.indexOf(l);d>-1&&(r.splice(d,1),s+=l+".")}),s+=o,r.length!=0||o.length===0)return null;let c={};return c.domEventName=i,c.fullKey=s,c}static matchEventFullKeyCode(t,r){let i=Zx[t.key]||t.key,o="";return r.indexOf("code.")>-1&&(i=t.code,o="code."),i==null||!i?!1:(i=i.toLowerCase(),i===" "?i="space":i==="."&&(i="dot"),Wy.forEach(s=>{if(s!==i){let a=Qx[s];a(t)&&(o+=s+".")}}),o+=i,o===r)}static eventCallback(t,r,i){return o=>{e.matchEventFullKeyCode(o,t)&&i.runGuarded(()=>r(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(r){return new(r||e)(E(U))};static \u0275prov=y({token:e,factory:e.\u0275fac})}return e})();async function mp(e,n,t){let r=v({rootComponent:e},Yx(n,t));return Ty(r)}function Yx(e,n){return{platformRef:n?.platformRef,appProviders:[...tM,...e?.providers??[]],platformProviders:eM}}function Kx(){Pc.makeCurrent()}function Xx(){return new Ke}function Jx(){return of(document),document}var eM=[{provide:Cr,useValue:ap},{provide:oc,useValue:Kx,multi:!0},{provide:U,useFactory:Jx}];var tM=[{provide:so,useValue:"root"},{provide:Ke,useFactory:Xx},{provide:Fc,useClass:Nc,multi:!0},{provide:Fc,useClass:qy,multi:!0},hp,fp,up,{provide:xt,useExisting:hp},{provide:Ir,useClass:qx},[]];var Ln=class e{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(t=>{let r=t.indexOf(":");if(r>0){let i=t.slice(0,r),o=t.slice(r+1).trim();this.addHeaderEntry(i,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((t,r)=>{this.addHeaderEntry(r,t)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([t,r])=>{this.setHeaderEntries(t,r)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let t=this.headers.get(n.toLowerCase());return t&&t.length>0?t[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,t){return this.clone({name:n,value:t,op:"a"})}set(n,t){return this.clone({name:n,value:t,op:"s"})}delete(n,t){return this.clone({name:n,value:t,op:"d"})}maybeSetNormalizedName(n,t){this.normalizedNames.has(t)||this.normalizedNames.set(t,n)}init(){this.lazyInit&&(this.lazyInit instanceof e?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(t=>{this.headers.set(t,n.headers.get(t)),this.normalizedNames.set(t,n.normalizedNames.get(t))})}clone(n){let t=new e;return t.lazyInit=this.lazyInit&&this.lazyInit instanceof e?this.lazyInit:this,t.lazyUpdate=(this.lazyUpdate||[]).concat([n]),t}applyUpdate(n){let t=n.name.toLowerCase();switch(n.op){case"a":case"s":let r=n.value;if(typeof r=="string"&&(r=[r]),r.length===0)return;this.maybeSetNormalizedName(n.name,t);let i=(n.op==="a"?this.headers.get(t):void 0)||[];i.push(...r),this.headers.set(t,i);break;case"d":let o=n.value;if(!o)this.headers.delete(t),this.normalizedNames.delete(t);else{let s=this.headers.get(t);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(t),this.normalizedNames.delete(t)):this.headers.set(t,s)}break}}addHeaderEntry(n,t){let r=n.toLowerCase();this.maybeSetNormalizedName(n,r),this.headers.has(r)?this.headers.get(r).push(t):this.headers.set(r,[t])}setHeaderEntries(n,t){let r=(Array.isArray(t)?t:[t]).map(o=>o.toString()),i=n.toLowerCase();this.headers.set(i,r),this.maybeSetNormalizedName(n,i)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(t=>n(this.normalizedNames.get(t),this.headers.get(t)))}};var jc=class{map=new Map;set(n,t){return this.map.set(n,t),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Vc=class{encodeKey(n){return Zy(n)}encodeValue(n){return Zy(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function nM(e,n){let t=new Map;return e.length>0&&e.replace(/^\?/,"").split("&").forEach(i=>{let o=i.indexOf("="),[s,a]=o==-1?[n.decodeKey(i),""]:[n.decodeKey(i.slice(0,o)),n.decodeValue(i.slice(o+1))],c=t.get(s)||[];c.push(a),t.set(s,c)}),t}var rM=/%(\d[a-f0-9])/gi,iM={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function Zy(e){return encodeURIComponent(e).replace(rM,(n,t)=>iM[t]??n)}function Lc(e){return`${e}`}var fn=class e{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Vc,n.fromString){if(n.fromObject)throw new b(2805,!1);this.map=nM(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(t=>{let r=n.fromObject[t],i=Array.isArray(r)?r.map(Lc):[Lc(r)];this.map.set(t,i)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let t=this.map.get(n);return t?t[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,t){return this.clone({param:n,value:t,op:"a"})}appendAll(n){let t=[];return Object.keys(n).forEach(r=>{let i=n[r];Array.isArray(i)?i.forEach(o=>{t.push({param:r,value:o,op:"a"})}):t.push({param:r,value:i,op:"a"})}),this.clone(t)}set(n,t){return this.clone({param:n,value:t,op:"s"})}delete(n,t){return this.clone({param:n,value:t,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let t=this.encoder.encodeKey(n);return this.map.get(n).map(r=>t+"="+this.encoder.encodeValue(r)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let t=new e({encoder:this.encoder});return t.cloneFrom=this.cloneFrom||this,t.updates=(this.updates||[]).concat(n),t}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let t=(n.op==="a"?this.map.get(n.param):void 0)||[];t.push(Lc(n.value)),this.map.set(n.param,t);break;case"d":if(n.value!==void 0){let r=this.map.get(n.param)||[],i=r.indexOf(Lc(n.value));i!==-1&&r.splice(i,1),r.length>0?this.map.set(n.param,r):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function oM(e){switch(e){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function Qy(e){return typeof ArrayBuffer<"u"&&e instanceof ArrayBuffer}function Yy(e){return typeof Blob<"u"&&e instanceof Blob}function Ky(e){return typeof FormData<"u"&&e instanceof FormData}function sM(e){return typeof URLSearchParams<"u"&&e instanceof URLSearchParams}var Xy="Content-Type",Jy="Accept",eb="text/plain",tb="application/json",aM=`${tb}, ${eb}, */*`,bi=class e{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,t,r,i){this.url=t,this.method=n.toUpperCase();let o;if(oM(this.method)||i?(this.body=r!==void 0?r:null,o=i):o=r,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new b(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Ln,this.context??=new jc,!this.params)this.params=new fn,this.urlWithParams=t;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=t;else{let a=t.indexOf("?"),c=a===-1?"?":a<t.length-1?"&":"";this.urlWithParams=t+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||Qy(this.body)||Yy(this.body)||Ky(this.body)||sM(this.body)?this.body:this.body instanceof fn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||Ky(this.body)?null:Yy(this.body)?this.body.type||null:Qy(this.body)?null:typeof this.body=="string"?eb:this.body instanceof fn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?tb:null}clone(n={}){let t=n.method||this.method,r=n.url||this.url,i=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,u=n.referrer||this.referrer,p=n.integrity||this.integrity,h=n.referrerPolicy||this.referrerPolicy,g=n.transferCache??this.transferCache,I=n.timeout??this.timeout,D=n.body!==void 0?n.body:this.body,w=n.withCredentials??this.withCredentials,ce=n.reportProgress??this.reportProgress,Qe=n.headers||this.headers,Ce=n.params||this.params,ji=n.context??this.context;return n.setHeaders!==void 0&&(Qe=Object.keys(n.setHeaders).reduce((Vi,Gn)=>Vi.set(Gn,n.setHeaders[Gn]),Qe)),n.setParams&&(Ce=Object.keys(n.setParams).reduce((Vi,Gn)=>Vi.set(Gn,n.setParams[Gn]),Ce)),new e(t,r,D,{params:Ce,headers:Qe,context:ji,reportProgress:ce,responseType:i,withCredentials:w,transferCache:g,keepalive:o,cache:a,priority:s,timeout:I,mode:c,redirect:l,credentials:d,referrer:u,integrity:p,referrerPolicy:h})}},xr=(function(e){return e[e.Sent=0]="Sent",e[e.UploadProgress=1]="UploadProgress",e[e.ResponseHeader=2]="ResponseHeader",e[e.DownloadProgress=3]="DownloadProgress",e[e.Response=4]="Response",e[e.User=5]="User",e})(xr||{}),Di=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,t=200,r="OK"){this.headers=n.headers||new Ln,this.status=n.status!==void 0?n.status:t,this.statusText=n.statusText||r,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Bc=class e extends Di{constructor(n={}){super(n)}type=xr.ResponseHeader;clone(n={}){return new e({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Uo=class e extends Di{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=xr.Response;clone(n={}){return new e({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},_i=class extends Di{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},cM=200,lM=204;var dM=new m("");var uM=/^\)\]\}',?\n/;var vp=(()=>{class e{xhrFactory;tracingService=f(Gt,{optional:!0});constructor(t){this.xhrFactory=t}maybePropagateTrace(t){return this.tracingService?.propagate?this.tracingService.propagate(t):t}handle(t){if(t.method==="JSONP")throw new b(-2800,!1);let r=this.xhrFactory;return M(null).pipe(Je(()=>new j(o=>{let s=r.build();if(s.open(t.method,t.urlWithParams),t.withCredentials&&(s.withCredentials=!0),t.headers.forEach((D,w)=>s.setRequestHeader(D,w.join(","))),t.headers.has(Jy)||s.setRequestHeader(Jy,aM),!t.headers.has(Xy)){let D=t.detectContentTypeHeader();D!==null&&s.setRequestHeader(Xy,D)}if(t.timeout&&(s.timeout=t.timeout),t.responseType){let D=t.responseType.toLowerCase();s.responseType=D!=="json"?D:"text"}let a=t.serializeBody(),c=null,l=()=>{if(c!==null)return c;let D=s.statusText||"OK",w=new Ln(s.getAllResponseHeaders()),ce=s.responseURL||t.url;return c=new Bc({headers:w,status:s.status,statusText:D,url:ce}),c},d=this.maybePropagateTrace(()=>{let{headers:D,status:w,statusText:ce,url:Qe}=l(),Ce=null;w!==lM&&(Ce=typeof s.response>"u"?s.responseText:s.response),w===0&&(w=Ce?cM:0);let ji=w>=200&&w<300;if(t.responseType==="json"&&typeof Ce=="string"){let Vi=Ce;Ce=Ce.replace(uM,"");try{Ce=Ce!==""?JSON.parse(Ce):null}catch(Gn){Ce=Vi,ji&&(ji=!1,Ce={error:Gn,text:Ce})}}ji?(o.next(new Uo({body:Ce,headers:D,status:w,statusText:ce,url:Qe||void 0})),o.complete()):o.error(new _i({error:Ce,headers:D,status:w,statusText:ce,url:Qe||void 0}))}),u=this.maybePropagateTrace(D=>{let{url:w}=l(),ce=new _i({error:D,status:s.status||0,statusText:s.statusText||"Unknown Error",url:w||void 0});o.error(ce)}),p=u;t.timeout&&(p=this.maybePropagateTrace(D=>{let{url:w}=l(),ce=new _i({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:w||void 0});o.error(ce)}));let h=!1,g=this.maybePropagateTrace(D=>{h||(o.next(l()),h=!0);let w={type:xr.DownloadProgress,loaded:D.loaded};D.lengthComputable&&(w.total=D.total),t.responseType==="text"&&s.responseText&&(w.partialText=s.responseText),o.next(w)}),I=this.maybePropagateTrace(D=>{let w={type:xr.UploadProgress,loaded:D.loaded};D.lengthComputable&&(w.total=D.total),o.next(w)});return s.addEventListener("load",d),s.addEventListener("error",u),s.addEventListener("timeout",p),s.addEventListener("abort",u),t.reportProgress&&(s.addEventListener("progress",g),a!==null&&s.upload&&s.upload.addEventListener("progress",I)),s.send(a),o.next({type:xr.Sent}),()=>{s.removeEventListener("error",u),s.removeEventListener("abort",u),s.removeEventListener("load",d),s.removeEventListener("timeout",p),t.reportProgress&&(s.removeEventListener("progress",g),a!==null&&s.upload&&s.upload.removeEventListener("progress",I)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(r){return new(r||e)(E(Ir))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function fM(e,n){return n(e)}function pM(e,n,t){return(r,i)=>we(t,()=>n(r,o=>e(o,i)))}var nb=new m("",{factory:()=>[]}),rb=new m(""),ib=new m("",{factory:()=>!0});var yp=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=E(vp),i},providedIn:"root"})}return e})();var Hc=(()=>{class e{backend;injector;chain=null;pendingTasks=f(Na);contributeToStability=f(ib);constructor(t,r){this.backend=t,this.injector=r}handle(t){if(this.chain===null){let r=Array.from(new Set([...this.injector.get(nb),...this.injector.get(rb,[])]));this.chain=r.reduceRight((i,o)=>pM(i,o,this.injector),fM)}if(this.contributeToStability){let r=this.pendingTasks.add();return this.chain(t,i=>this.backend.handle(i)).pipe(tr(r))}else return this.chain(t,r=>this.backend.handle(r))}static \u0275fac=function(r){return new(r||e)(E(yp),E(ue))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),bp=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=E(Hc),i},providedIn:"root"})}return e})();function gp(e,n){return{body:n,headers:e.headers,context:e.context,observe:e.observe,params:e.params,reportProgress:e.reportProgress,responseType:e.responseType,withCredentials:e.withCredentials,credentials:e.credentials,transferCache:e.transferCache,timeout:e.timeout,keepalive:e.keepalive,priority:e.priority,cache:e.cache,mode:e.mode,redirect:e.redirect,integrity:e.integrity,referrer:e.referrer,referrerPolicy:e.referrerPolicy}}var Ci=(()=>{class e{handler;constructor(t){this.handler=t}request(t,r,i={}){let o;if(t instanceof bi)o=t;else{let c;i.headers instanceof Ln?c=i.headers:c=new Ln(i.headers);let l;i.params&&(i.params instanceof fn?l=i.params:l=new fn({fromObject:i.params})),o=new bi(t,r,i.body!==void 0?i.body:null,{headers:c,context:i.context,params:l,reportProgress:i.reportProgress,responseType:i.responseType||"json",withCredentials:i.withCredentials,transferCache:i.transferCache,keepalive:i.keepalive,priority:i.priority,cache:i.cache,mode:i.mode,redirect:i.redirect,credentials:i.credentials,referrer:i.referrer,referrerPolicy:i.referrerPolicy,integrity:i.integrity,timeout:i.timeout})}let s=M(o).pipe($r(c=>this.handler.handle(c)));if(t instanceof bi||i.observe==="events")return s;let a=s.pipe(Se(c=>c instanceof Uo));switch(i.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(L(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new b(2806,!1);return c.body}));case"blob":return a.pipe(L(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new b(2807,!1);return c.body}));case"text":return a.pipe(L(c=>{if(c.body!==null&&typeof c.body!="string")throw new b(2808,!1);return c.body}));default:return a.pipe(L(c=>c.body))}case"response":return a;default:throw new b(2809,!1)}}delete(t,r={}){return this.request("DELETE",t,r)}get(t,r={}){return this.request("GET",t,r)}head(t,r={}){return this.request("HEAD",t,r)}jsonp(t,r){return this.request("JSONP",t,{params:new fn().append(r,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(t,r={}){return this.request("OPTIONS",t,r)}patch(t,r,i={}){return this.request("PATCH",t,gp(i,r))}post(t,r,i={}){return this.request("POST",t,gp(i,r))}put(t,r,i={}){return this.request("PUT",t,gp(i,r))}static \u0275fac=function(r){return new(r||e)(E(bp))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var hM=new m("",{factory:()=>!0}),mM="XSRF-TOKEN",gM=new m("",{factory:()=>mM}),vM="X-XSRF-TOKEN",yM=new m("",{factory:()=>vM}),bM=(()=>{class e{cookieName=f(gM);doc=f(U);lastCookieString="";lastToken=null;parseCount=0;getToken(){let t=this.doc.cookie||"";return t!==this.lastCookieString&&(this.parseCount++,this.lastToken=Lo(t,this.cookieName),this.lastCookieString=t),this.lastToken}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),ob=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=E(bM),i},providedIn:"root"})}return e})();function _M(e,n){if(!f(hM)||e.method==="GET"||e.method==="HEAD")return n(e);try{let i=f(vi).href,{origin:o}=new URL(i),{origin:s}=new URL(e.url,o);if(o!==s)return n(e)}catch{return n(e)}let t=f(ob).getToken(),r=f(yM);return t!=null&&!e.headers.has(r)&&(e=e.clone({headers:e.headers.set(r,t)})),n(e)}function _p(...e){let n=[Ci,Hc,{provide:bp,useExisting:Hc},{provide:yp,useFactory:()=>f(dM,{optional:!0})??f(vp)},{provide:nb,useValue:_M,multi:!0}];for(let t of e)n.push(...t.\u0275providers);return cr(n)}var ab=(()=>{class e{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(r){return new(r||e)(E(U))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var Dp=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:function(r){let i=null;return r?i=new(r||e):i=E(DM),i},providedIn:"root"})}return e})(),DM=(()=>{class e extends Dp{_doc;constructor(t){super(),this._doc=t}sanitize(t,r){if(r==null)return null;switch(t){case Fe.NONE:return r;case Fe.HTML:return On(r,"HTML")?zt(r):pf(this._doc,String(r)).toString();case Fe.STYLE:return On(r,"Style")?zt(r):r;case Fe.SCRIPT:if(On(r,"Script"))return zt(r);throw new b(5200,!1);case Fe.URL:return On(r,"URL")?zt(r):wo(String(r));case Fe.RESOURCE_URL:if(On(r,"ResourceURL"))return zt(r);throw new b(5201,!1);default:throw new b(5202,!1)}}bypassSecurityTrustHtml(t){return cf(t)}bypassSecurityTrustStyle(t){return lf(t)}bypassSecurityTrustScript(t){return df(t)}bypassSecurityTrustUrl(t){return uf(t)}bypassSecurityTrustResourceUrl(t){return ff(t)}static \u0275fac=function(r){return new(r||e)(E(U))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var N="primary",ns=Symbol("RouteTitle"),xp=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t[0]:t}return null}getAll(n){if(this.has(n)){let t=this.params[n];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function Sr(e){return new xp(e)}function Cp(e,n,t){for(let r=0;r<e.length;r++){let i=e[r],o=n[r];if(i[0]===":")t[i.substring(1)]=o;else if(i!==o.path)return!1}return!0}function gb(e,n,t){let r=t.path.split("/"),i=r.indexOf("**");if(i===-1){if(r.length>e.length||t.pathMatch==="full"&&(n.hasChildren()||r.length<e.length))return null;let c={},l=e.slice(0,r.length);return Cp(r,l,c)?{consumed:l,posParams:c}:null}if(i!==r.lastIndexOf("**"))return null;let o=r.slice(0,i),s=r.slice(i+1);if(o.length+s.length>e.length||t.pathMatch==="full"&&n.hasChildren()&&t.path!=="**")return null;let a={};return!Cp(o,e.slice(0,o.length),a)||!Cp(s,e.slice(e.length-s.length),a)?null:{consumed:e,posParams:a}}function qc(e){return new Promise((n,t)=>{e.pipe(Xt()).subscribe({next:r=>n(r),error:r=>t(r)})})}function CM(e,n){if(e.length!==n.length)return!1;for(let t=0;t<e.length;++t)if(!Yt(e[t],n[t]))return!1;return!0}function Yt(e,n){let t=e?Mp(e):void 0,r=n?Mp(n):void 0;if(!t||!r||t.length!=r.length)return!1;let i;for(let o=0;o<t.length;o++)if(i=t[o],!vb(e[i],n[i]))return!1;return!0}function Mp(e){return[...Object.keys(e),...Object.getOwnPropertySymbols(e)]}function vb(e,n){if(Array.isArray(e)&&Array.isArray(n)){if(e.length!==n.length)return!1;let t=[...e].sort(),r=[...n].sort();return t.every((i,o)=>r[o]===i)}else return e===n}function EM(e){return e.length>0?e[e.length-1]:null}function Rr(e){return Xs(e)?e:kn(e)?de(Promise.resolve(e)):M(e)}function yb(e){return Xs(e)?qc(e):Promise.resolve(e)}var wM={exact:Db,subset:Cb},bb={exact:IM,subset:xM,ignored:()=>!0},_b={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Sp={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function lb(e,n,t){return wM[t.paths](e.root,n.root,t.matrixParams)&&bb[t.queryParams](e.queryParams,n.queryParams)&&!(t.fragment==="exact"&&e.fragment!==n.fragment)}function IM(e,n){return Yt(e,n)}function Db(e,n,t){if(!Mr(e.segments,n.segments)||!zc(e.segments,n.segments,t)||e.numberOfChildren!==n.numberOfChildren)return!1;for(let r in n.children)if(!e.children[r]||!Db(e.children[r],n.children[r],t))return!1;return!0}function xM(e,n){return Object.keys(n).length<=Object.keys(e).length&&Object.keys(n).every(t=>vb(e[t],n[t]))}function Cb(e,n,t){return Eb(e,n,n.segments,t)}function Eb(e,n,t,r){if(e.segments.length>t.length){let i=e.segments.slice(0,t.length);return!(!Mr(i,t)||n.hasChildren()||!zc(i,t,r))}else if(e.segments.length===t.length){if(!Mr(e.segments,t)||!zc(e.segments,t,r))return!1;for(let i in n.children)if(!e.children[i]||!Cb(e.children[i],n.children[i],r))return!1;return!0}else{let i=t.slice(0,e.segments.length),o=t.slice(e.segments.length);return!Mr(e.segments,i)||!zc(e.segments,i,r)||!e.children[N]?!1:Eb(e.children[N],n,o,r)}}function zc(e,n,t){return n.every((r,i)=>bb[t](e[i].parameters,r.parameters))}var ft=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Y([],{}),t={},r=null){this.root=n,this.queryParams=t,this.fragment=r}get queryParamMap(){return this._queryParamMap??=Sr(this.queryParams),this._queryParamMap}toString(){return TM.serialize(this)}},Y=class{segments;children;parent=null;constructor(n,t){this.segments=n,this.children=t,Object.values(t).forEach(r=>r.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Gc(this)}},jn=class{path;parameters;_parameterMap;constructor(n,t){this.path=n,this.parameters=t}get parameterMap(){return this._parameterMap??=Sr(this.parameters),this._parameterMap}toString(){return Ib(this)}};function MM(e,n){return Mr(e,n)&&e.every((t,r)=>Yt(t.parameters,n[r].parameters))}function Mr(e,n){return e.length!==n.length?!1:e.every((t,r)=>t.path===n[r].path)}function SM(e,n){let t=[];return Object.entries(e.children).forEach(([r,i])=>{r===N&&(t=t.concat(n(i,r)))}),Object.entries(e.children).forEach(([r,i])=>{r!==N&&(t=t.concat(n(i,r)))}),t}var rs=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>new Vn,providedIn:"root"})}return e})(),Vn=class{parse(n){let t=new Ap(n);return new ft(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(n){let t=`/${$o(n.root,!0)}`,r=NM(n.queryParams),i=typeof n.fragment=="string"?`#${AM(n.fragment)}`:"";return`${t}${r}${i}`}},TM=new Vn;function Gc(e){return e.segments.map(n=>Ib(n)).join("/")}function $o(e,n){if(!e.hasChildren())return Gc(e);if(n){let t=e.children[N]?$o(e.children[N],!1):"",r=[];return Object.entries(e.children).forEach(([i,o])=>{i!==N&&r.push(`${i}:${$o(o,!1)}`)}),r.length>0?`${t}(${r.join("//")})`:t}else{let t=SM(e,(r,i)=>i===N?[$o(e.children[N],!1)]:[`${i}:${$o(r,!1)}`]);return Object.keys(e.children).length===1&&e.children[N]!=null?`${Gc(e)}/${t[0]}`:`${Gc(e)}/(${t.join("//")})`}}function wb(e){return encodeURIComponent(e).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Uc(e){return wb(e).replace(/%3B/gi,";")}function AM(e){return encodeURI(e)}function Tp(e){return wb(e).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Wc(e){return decodeURIComponent(e)}function db(e){return Wc(e.replace(/\+/g,"%20"))}function Ib(e){return`${Tp(e.path)}${RM(e.parameters)}`}function RM(e){return Object.entries(e).map(([n,t])=>`;${Tp(n)}=${Tp(t)}`).join("")}function NM(e){let n=Object.entries(e).map(([t,r])=>Array.isArray(r)?r.map(i=>`${Uc(t)}=${Uc(i)}`).join("&"):`${Uc(t)}=${Uc(r)}`).filter(t=>t);return n.length?`?${n.join("&")}`:""}var OM=/^[^\/()?;#]+/;function Ep(e){let n=e.match(OM);return n?n[0]:""}var kM=/^[^\/()?;=#]+/;function FM(e){let n=e.match(kM);return n?n[0]:""}var PM=/^[^=?&#]+/;function LM(e){let n=e.match(PM);return n?n[0]:""}var jM=/^[^&#]+/;function VM(e){let n=e.match(jM);return n?n[0]:""}var Ap=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Y([],{}):new Y([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new b(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let r={};this.peekStartsWith("/(")&&(this.capture("/"),r=this.parseParens(!0,n));let i={};return this.peekStartsWith("(")&&(i=this.parseParens(!1,n)),(t.length>0||Object.keys(r).length>0)&&(i[N]=new Y(t,r)),i}parseSegment(){let n=Ep(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new b(4009,!1);return this.capture(n),new jn(Wc(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let t=FM(this.remaining);if(!t)return;this.capture(t);let r="";if(this.consumeOptional("=")){let i=Ep(this.remaining);i&&(r=i,this.capture(r))}n[Wc(t)]=Wc(r)}parseQueryParam(n){let t=LM(this.remaining);if(!t)return;this.capture(t);let r="";if(this.consumeOptional("=")){let s=VM(this.remaining);s&&(r=s,this.capture(r))}let i=db(t),o=db(r);if(n.hasOwnProperty(i)){let s=n[i];Array.isArray(s)||(s=[s],n[i]=s),s.push(o)}else n[i]=o}parseParens(n,t){let r={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let i=Ep(this.remaining),o=this.remaining[i.length];if(o!=="/"&&o!==")"&&o!==";")throw new b(4010,!1);let s;i.indexOf(":")>-1?(s=i.slice(0,i.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=N);let a=this.parseChildren(t+1);r[s??N]=Object.keys(a).length===1&&a[N]?a[N]:new Y([],a),this.consumeOptional("//")}return r}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new b(4011,!1)}};function xb(e){return e.segments.length>0?new Y([],{[N]:e}):e}function Mb(e){let n={};for(let[r,i]of Object.entries(e.children)){let o=Mb(i);if(r===N&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[r]=o)}let t=new Y(e.segments,n);return BM(t)}function BM(e){if(e.numberOfChildren===1&&e.children[N]){let n=e.children[N];return new Y(e.segments.concat(n.segments),n.children)}return e}function xi(e){return e instanceof ft}function Sb(e,n,t=null,r=null,i=new Vn){let o=Tb(e);return Ab(o,n,t,r,i)}function Tb(e){let n;function t(o){let s={};for(let c of o.children){let l=t(c);s[c.outlet]=l}let a=new Y(o.url,s);return o===e&&(n=a),a}let r=t(e.root),i=xb(r);return n??i}function Ab(e,n,t,r,i){let o=e;for(;o.parent;)o=o.parent;if(n.length===0)return wp(o,o,o,t,r,i);let s=HM(n);if(s.toRoot())return wp(o,o,new Y([],{}),t,r,i);let a=UM(s,o,e),c=a.processChildren?Go(a.segmentGroup,a.index,s.commands):Nb(a.segmentGroup,a.index,s.commands);return wp(o,a.segmentGroup,c,t,r,i)}function Zc(e){return typeof e=="object"&&e!=null&&!e.outlets&&!e.segmentPath}function Zo(e){return typeof e=="object"&&e!=null&&e.outlets}function ub(e,n,t){e||="\u0275";let r=new ft;return r.queryParams={[e]:n},t.parse(t.serialize(r)).queryParams[e]}function wp(e,n,t,r,i,o){let s={};for(let[l,d]of Object.entries(r??{}))s[l]=Array.isArray(d)?d.map(u=>ub(l,u,o)):ub(l,d,o);let a;e===n?a=t:a=Rb(e,n,t);let c=xb(Mb(a));return new ft(c,s,i)}function Rb(e,n,t){let r={};return Object.entries(e.children).forEach(([i,o])=>{o===n?r[i]=t:r[i]=Rb(o,n,t)}),new Y(e.segments,r)}var Qc=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,t,r){if(this.isAbsolute=n,this.numberOfDoubleDots=t,this.commands=r,n&&r.length>0&&Zc(r[0]))throw new b(4003,!1);let i=r.find(Zo);if(i&&i!==EM(r))throw new b(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function HM(e){if(typeof e[0]=="string"&&e.length===1&&e[0]==="/")return new Qc(!0,0,e);let n=0,t=!1,r=e.reduce((i,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...i,{outlets:a}]}if(o.segmentPath)return[...i,o.segmentPath]}return typeof o!="string"?[...i,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?n++:a!=""&&i.push(a))}),i):[...i,o]},[]);return new Qc(t,n,r)}var wi=class{segmentGroup;processChildren;index;constructor(n,t,r){this.segmentGroup=n,this.processChildren=t,this.index=r}};function UM(e,n,t){if(e.isAbsolute)return new wi(n,!0,0);if(!t)return new wi(n,!1,NaN);if(t.parent===null)return new wi(t,!0,0);let r=Zc(e.commands[0])?0:1,i=t.segments.length-1+r;return $M(t,i,e.numberOfDoubleDots)}function $M(e,n,t){let r=e,i=n,o=t;for(;o>i;){if(o-=i,r=r.parent,!r)throw new b(4005,!1);i=r.segments.length}return new wi(r,!1,i-o)}function zM(e){return Zo(e[0])?e[0].outlets:{[N]:e}}function Nb(e,n,t){if(e??=new Y([],{}),e.segments.length===0&&e.hasChildren())return Go(e,n,t);let r=GM(e,n,t),i=t.slice(r.commandIndex);if(r.match&&r.pathIndex<e.segments.length){let o=new Y(e.segments.slice(0,r.pathIndex),{});return o.children[N]=new Y(e.segments.slice(r.pathIndex),e.children),Go(o,0,i)}else return r.match&&i.length===0?new Y(e.segments,{}):r.match&&!e.hasChildren()?Rp(e,n,t):r.match?Go(e,0,i):Rp(e,n,t)}function Go(e,n,t){if(t.length===0)return new Y(e.segments,{});{let r=zM(t),i={};if(Object.keys(r).some(o=>o!==N)&&e.children[N]&&e.numberOfChildren===1&&e.children[N].segments.length===0){let o=Go(e.children[N],n,t);return new Y(e.segments,o.children)}return Object.entries(r).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(i[o]=Nb(e.children[o],n,s))}),Object.entries(e.children).forEach(([o,s])=>{r[o]===void 0&&(i[o]=s)}),new Y(e.segments,i)}}function GM(e,n,t){let r=0,i=n,o={match:!1,pathIndex:0,commandIndex:0};for(;i<e.segments.length;){if(r>=t.length)return o;let s=e.segments[i],a=t[r];if(Zo(a))break;let c=`${a}`,l=r<t.length-1?t[r+1]:null;if(i>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!pb(c,l,s))return o;r+=2}else{if(!pb(c,{},s))return o;r++}i++}return{match:!0,pathIndex:i,commandIndex:r}}function Rp(e,n,t){let r=e.segments.slice(0,n),i=0;for(;i<t.length;){let o=t[i];if(Zo(o)){let c=WM(o.outlets);return new Y(r,c)}if(i===0&&Zc(t[0])){let c=e.segments[n];r.push(new jn(c.path,fb(t[0]))),i++;continue}let s=Zo(o)?o.outlets[N]:`${o}`,a=i<t.length-1?t[i+1]:null;s&&a&&Zc(a)?(r.push(new jn(s,fb(a))),i+=2):(r.push(new jn(s,{})),i++)}return new Y(r,{})}function WM(e){let n={};return Object.entries(e).forEach(([t,r])=>{typeof r=="string"&&(r=[r]),r!==null&&(n[t]=Rp(new Y([],{}),0,r))}),n}function fb(e){let n={};return Object.entries(e).forEach(([t,r])=>n[t]=`${r}`),n}function pb(e,n,t){return e==t.path&&Yt(n,t.parameters)}var Wo="imperative",Me=(function(e){return e[e.NavigationStart=0]="NavigationStart",e[e.NavigationEnd=1]="NavigationEnd",e[e.NavigationCancel=2]="NavigationCancel",e[e.NavigationError=3]="NavigationError",e[e.RoutesRecognized=4]="RoutesRecognized",e[e.ResolveStart=5]="ResolveStart",e[e.ResolveEnd=6]="ResolveEnd",e[e.GuardsCheckStart=7]="GuardsCheckStart",e[e.GuardsCheckEnd=8]="GuardsCheckEnd",e[e.RouteConfigLoadStart=9]="RouteConfigLoadStart",e[e.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",e[e.ChildActivationStart=11]="ChildActivationStart",e[e.ChildActivationEnd=12]="ChildActivationEnd",e[e.ActivationStart=13]="ActivationStart",e[e.ActivationEnd=14]="ActivationEnd",e[e.Scroll=15]="Scroll",e[e.NavigationSkipped=16]="NavigationSkipped",e})(Me||{}),it=class{id;url;constructor(n,t){this.id=n,this.url=t}},Tr=class extends it{type=Me.NavigationStart;navigationTrigger;restoredState;constructor(n,t,r="imperative",i=null){super(n,t),this.navigationTrigger=r,this.restoredState=i}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},hn=class extends it{urlAfterRedirects;type=Me.NavigationEnd;constructor(n,t,r){super(n,t),this.urlAfterRedirects=r}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Be=(function(e){return e[e.Redirect=0]="Redirect",e[e.SupersededByNewNavigation=1]="SupersededByNewNavigation",e[e.NoDataFromResolver=2]="NoDataFromResolver",e[e.GuardRejected=3]="GuardRejected",e[e.Aborted=4]="Aborted",e})(Be||{}),Qo=(function(e){return e[e.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",e[e.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",e})(Qo||{}),ut=class extends it{reason;code;type=Me.NavigationCancel;constructor(n,t,r,i){super(n,t),this.reason=r,this.code=i}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function Ob(e){return e instanceof ut&&(e.code===Be.Redirect||e.code===Be.SupersededByNewNavigation)}var mn=class extends it{reason;code;type=Me.NavigationSkipped;constructor(n,t,r,i){super(n,t),this.reason=r,this.code=i}},Ar=class extends it{error;target;type=Me.NavigationError;constructor(n,t,r,i){super(n,t),this.error=r,this.target=i}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Yo=class extends it{urlAfterRedirects;state;type=Me.RoutesRecognized;constructor(n,t,r,i){super(n,t),this.urlAfterRedirects=r,this.state=i}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Yc=class extends it{urlAfterRedirects;state;type=Me.GuardsCheckStart;constructor(n,t,r,i){super(n,t),this.urlAfterRedirects=r,this.state=i}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Kc=class extends it{urlAfterRedirects;state;shouldActivate;type=Me.GuardsCheckEnd;constructor(n,t,r,i,o){super(n,t),this.urlAfterRedirects=r,this.state=i,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Xc=class extends it{urlAfterRedirects;state;type=Me.ResolveStart;constructor(n,t,r,i){super(n,t),this.urlAfterRedirects=r,this.state=i}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Jc=class extends it{urlAfterRedirects;state;type=Me.ResolveEnd;constructor(n,t,r,i){super(n,t),this.urlAfterRedirects=r,this.state=i}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},el=class{route;type=Me.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},tl=class{route;type=Me.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},nl=class{snapshot;type=Me.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},rl=class{snapshot;type=Me.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},il=class{snapshot;type=Me.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ol=class{snapshot;type=Me.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Mi=class{},Ko=class{},Si=class{url;navigationBehaviorOptions;constructor(n,t){this.url=n,this.navigationBehaviorOptions=t}};function qM(e){return!(e instanceof Mi)&&!(e instanceof Si)&&!(e instanceof Ko)}var sl=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Ni(this.rootInjector)}},Ni=(()=>{class e{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,r){let i=this.getOrCreateContext(t);i.outlet=r,this.contexts.set(t,i)}onChildOutletDestroyed(t){let r=this.getContext(t);r&&(r.outlet=null,r.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let r=this.getContext(t);return r||(r=new sl(this.rootInjector),this.contexts.set(t,r)),r}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(r){return new(r||e)(E(ue))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),al=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let t=this.pathFromRoot(n);return t.length>1?t[t.length-2]:null}children(n){let t=Np(n,this._root);return t?t.children.map(r=>r.value):[]}firstChild(n){let t=Np(n,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(n){let t=Op(n,this._root);return t.length<2?[]:t[t.length-2].children.map(i=>i.value).filter(i=>i!==n)}pathFromRoot(n){return Op(n,this._root).map(t=>t.value)}};function Np(e,n){if(e===n.value)return n;for(let t of n.children){let r=Np(e,t);if(r)return r}return null}function Op(e,n){if(e===n.value)return[n];for(let t of n.children){let r=Op(e,t);if(r.length)return r.unshift(n),r}return[]}var rt=class{value;children;constructor(n,t){this.value=n,this.children=t}toString(){return`TreeNode(${this.value})`}};function Ei(e){let n={};return e&&e.children.forEach(t=>n[t.value.outlet]=t),n}var Xo=class extends al{snapshot;constructor(n,t){super(n),this.snapshot=t,Up(this,n)}toString(){return this.snapshot.toString()}};function kb(e,n){let t=ZM(e,n),r=new _e([new jn("",{})]),i=new _e({}),o=new _e({}),s=new _e({}),a=new _e(""),c=new Bn(r,i,s,a,o,N,e,t.root);return c.snapshot=t.root,new Xo(new rt(c,[]),t)}function ZM(e,n){let t={},r={},i={},s=new Ti([],t,i,"",r,N,e,null,{},n);return new Jo("",new rt(s,[]))}var Bn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,t,r,i,o,s,a,c){this.urlSubject=n,this.paramsSubject=t,this.queryParamsSubject=r,this.fragmentSubject=i,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(L(l=>l[ns]))??M(void 0),this.url=n,this.params=t,this.queryParams=r,this.fragment=i,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(L(n=>Sr(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(L(n=>Sr(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Hp(e,n,t="emptyOnly"){let r,{routeConfig:i}=e;return n!==null&&(t==="always"||i?.path===""||!n.component&&!n.routeConfig?.loadComponent)?r={params:v(v({},n.params),e.params),data:v(v({},n.data),e.data),resolve:v(v(v(v({},e.data),n.data),i?.data),e._resolvedData)}:r={params:v({},e.params),data:v({},e.data),resolve:v(v({},e.data),e._resolvedData??{})},i&&Pb(i)&&(r.resolve[ns]=i.title),r}var Ti=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[ns]}constructor(n,t,r,i,o,s,a,c,l,d){this.url=n,this.params=t,this.queryParams=r,this.fragment=i,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Sr(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Sr(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(r=>r.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${t}')`}},Jo=class extends al{url;constructor(n,t){super(t),this.url=n,Up(this,t)}toString(){return Fb(this._root)}};function Up(e,n){n.value._routerState=e,n.children.forEach(t=>Up(e,t))}function Fb(e){let n=e.children.length>0?` { ${e.children.map(Fb).join(", ")} } `:"";return`${e.value}${n}`}function Ip(e){if(e.snapshot){let n=e.snapshot,t=e._futureSnapshot;e.snapshot=t,Yt(n.queryParams,t.queryParams)||e.queryParamsSubject.next(t.queryParams),n.fragment!==t.fragment&&e.fragmentSubject.next(t.fragment),Yt(n.params,t.params)||e.paramsSubject.next(t.params),CM(n.url,t.url)||e.urlSubject.next(t.url),Yt(n.data,t.data)||e.dataSubject.next(t.data)}else e.snapshot=e._futureSnapshot,e.dataSubject.next(e._futureSnapshot.data)}function kp(e,n){let t=Yt(e.params,n.params)&&MM(e.url,n.url),r=!e.parent!=!n.parent;return t&&!r&&(!e.parent||kp(e.parent,n.parent))}function Pb(e){return typeof e.title=="string"||e.title===null}var Lb=new m(""),is=(()=>{class e{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=N;activateEvents=new re;deactivateEvents=new re;attachEvents=new re;detachEvents=new re;routerOutletData=My();parentContexts=f(Ni);location=f(ln);changeDetector=f(Rt);inputBinder=f(ul,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:r,previousValue:i}=t.name;if(r)return;this.isTrackedInParentContexts(i)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(i)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new b(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new b(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new b(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,r){this.activated=t,this._activatedRoute=r,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,r){if(this.isActivated)throw new b(4013,!1);this._activatedRoute=t;let i=this.location,s=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Fp(t,a,i.injector,this.routerOutletData);this.activated=i.createComponent(s,{index:i.length,injector:c,environmentInjector:r}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[sn]})}return e})(),Fp=class{route;childContexts;parent;outletData;constructor(n,t,r,i){this.route=n,this.childContexts=t,this.parent=r,this.outletData=i}get(n,t){return n===Bn?this.route:n===Ni?this.childContexts:n===Lb?this.outletData:this.parent.get(n,t)}},ul=new m("");var $p=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(r,i){r&1&&ae(0,"router-outlet")},dependencies:[is],encapsulation:2})}return e})();function zp(e){let n=e.children&&e.children.map(zp),t=n?P(v({},e),{children:n}):v({},e);return!t.component&&!t.loadComponent&&(n||t.loadChildren)&&t.outlet&&t.outlet!==N&&(t.component=$p),t}function QM(e,n,t){let r=es(e,n._root,t?t._root:void 0);return new Xo(r,n)}function es(e,n,t){if(t&&e.shouldReuseRoute(n.value,t.value.snapshot)){let r=t.value;r._futureSnapshot=n.value;let i=YM(e,n,t);return new rt(r,i)}else{if(e.shouldAttach(n.value)){let o=e.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>es(e,a)),s}}let r=KM(n.value),i=n.children.map(o=>es(e,o));return new rt(r,i)}}function YM(e,n,t){return n.children.map(r=>{for(let i of t.children)if(e.shouldReuseRoute(r.value,i.value.snapshot))return es(e,r,i);return es(e,r)})}function KM(e){return new Bn(new _e(e.url),new _e(e.params),new _e(e.queryParams),new _e(e.fragment),new _e(e.data),e.outlet,e.component,e)}var Ai=class{redirectTo;navigationBehaviorOptions;constructor(n,t){this.redirectTo=n,this.navigationBehaviorOptions=t}},jb="ngNavigationCancelingError";function cl(e,n){let{redirectTo:t,navigationBehaviorOptions:r}=xi(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,i=Vb(!1,Be.Redirect);return i.url=t,i.navigationBehaviorOptions=r,i}function Vb(e,n){let t=new Error(`NavigationCancelingError: ${e||""}`);return t[jb]=!0,t.cancellationCode=n,t}function XM(e){return Bb(e)&&xi(e.url)}function Bb(e){return!!e&&e[jb]}var Pp=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,t,r,i,o){this.routeReuseStrategy=n,this.futureState=t,this.currState=r,this.forwardEvent=i,this.inputBindingEnabled=o}activate(n){let t=this.futureState._root,r=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,r,n),Ip(this.futureState.root),this.activateChildRoutes(t,r,n)}deactivateChildRoutes(n,t,r){let i=Ei(t);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,i[s],r),delete i[s]}),Object.values(i).forEach(o=>{this.deactivateRouteAndItsChildren(o,r)})}deactivateRoutes(n,t,r){let i=n.value,o=t?t.value:null;if(i===o)if(i.component){let s=r.getContext(i.outlet);s&&this.deactivateChildRoutes(n,t,s.children)}else this.deactivateChildRoutes(n,t,r);else o&&this.deactivateRouteAndItsChildren(t,r)}deactivateRouteAndItsChildren(n,t){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,t):this.deactivateRouteAndOutlet(n,t)}detachAndStoreRouteSubtree(n,t){let r=t.getContext(n.value.outlet),i=r&&n.value.component?r.children:t,o=Ei(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,i);if(r&&r.outlet){let s=r.outlet.detach(),a=r.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,t){let r=t.getContext(n.value.outlet),i=r&&n.value.component?r.children:t,o=Ei(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,i);r&&(r.outlet&&(r.outlet.deactivate(),r.children.onOutletDeactivated()),r.attachRef=null,r.route=null)}activateChildRoutes(n,t,r){let i=Ei(t);n.children.forEach(o=>{this.activateRoutes(o,i[o.value.outlet],r),this.forwardEvent(new ol(o.value.snapshot))}),n.children.length&&this.forwardEvent(new rl(n.value.snapshot))}activateRoutes(n,t,r){let i=n.value,o=t?t.value:null;if(Ip(i),i===o)if(i.component){let s=r.getOrCreateContext(i.outlet);this.activateChildRoutes(n,t,s.children)}else this.activateChildRoutes(n,t,r);else if(i.component){let s=r.getOrCreateContext(i.outlet);if(this.routeReuseStrategy.shouldAttach(i.snapshot)){let a=this.routeReuseStrategy.retrieve(i.snapshot);this.routeReuseStrategy.store(i.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Ip(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=i,s.outlet&&s.outlet.activateWith(i,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,r)}},ll=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Ii=class{component;route;constructor(n,t){this.component=n,this.route=t}};function JM(e,n,t){let r=e._root,i=n?n._root:null;return zo(r,i,t,[r.value])}function eS(e){let n=e.routeConfig?e.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:e,guards:n}}function Oi(e,n){let t=Symbol(),r=n.get(e,t);return r===t?typeof e=="function"&&!Ed(e)?e:n.get(e):r}function zo(e,n,t,r,i={canDeactivateChecks:[],canActivateChecks:[]}){let o=Ei(n);return e.children.forEach(s=>{tS(s,o[s.value.outlet],t,r.concat([s.value]),i),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>qo(a,t.getContext(s),i)),i}function tS(e,n,t,r,i={canDeactivateChecks:[],canActivateChecks:[]}){let o=e.value,s=n?n.value:null,a=t?t.getContext(e.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=nS(s,o,o.routeConfig.runGuardsAndResolvers);c?i.canActivateChecks.push(new ll(r)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?zo(e,n,a?a.children:null,r,i):zo(e,n,t,r,i),c&&a&&a.outlet&&a.outlet.isActivated&&i.canDeactivateChecks.push(new Ii(a.outlet.component,s))}else s&&qo(n,a,i),i.canActivateChecks.push(new ll(r)),o.component?zo(e,null,a?a.children:null,r,i):zo(e,null,t,r,i);return i}function nS(e,n,t){if(typeof t=="function")return we(n._environmentInjector,()=>t(e,n));switch(t){case"pathParamsChange":return!Mr(e.url,n.url);case"pathParamsOrQueryParamsChange":return!Mr(e.url,n.url)||!Yt(e.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!kp(e,n)||!Yt(e.queryParams,n.queryParams);default:return!kp(e,n)}}function qo(e,n,t){let r=Ei(e),i=e.value;Object.entries(r).forEach(([o,s])=>{i.component?n?qo(s,n.children.getContext(o),t):qo(s,null,t):qo(s,n,t)}),i.component?n&&n.outlet&&n.outlet.isActivated?t.canDeactivateChecks.push(new Ii(n.outlet.component,i)):t.canDeactivateChecks.push(new Ii(null,i)):t.canDeactivateChecks.push(new Ii(null,i))}function os(e){return typeof e=="function"}function rS(e){return typeof e=="boolean"}function iS(e){return e&&os(e.canLoad)}function oS(e){return e&&os(e.canActivate)}function sS(e){return e&&os(e.canActivateChild)}function aS(e){return e&&os(e.canDeactivate)}function cS(e){return e&&os(e.canMatch)}function Hb(e){return e instanceof er||e?.name==="EmptyError"}var $c=Symbol("INITIAL_VALUE");function Ri(){return Je(e=>nd(e.map(n=>n.pipe(mt(1),Yi($c)))).pipe(L(n=>{for(let t of n)if(t!==!0){if(t===$c)return $c;if(t===!1||lS(t))return t}return!0}),Se(n=>n!==$c),mt(1)))}function lS(e){return xi(e)||e instanceof Ai}function Ub(e){return e.aborted?M(void 0).pipe(mt(1)):new j(n=>{let t=()=>{n.next(),n.complete()};return e.addEventListener("abort",t),()=>e.removeEventListener("abort",t)})}function $b(e){return gt(Ub(e))}function dS(e){return $e(n=>{let{targetSnapshot:t,currentSnapshot:r,guards:{canActivateChecks:i,canDeactivateChecks:o}}=n;return o.length===0&&i.length===0?M(P(v({},n),{guardsResult:!0})):uS(o,t,r).pipe($e(s=>s&&rS(s)?fS(t,i,e):M(s)),L(s=>P(v({},n),{guardsResult:s})))})}function uS(e,n,t){return de(e).pipe($e(r=>vS(r.component,r.route,t,n)),Xt(r=>r!==!0,!0))}function fS(e,n,t){return de(n).pipe($r(r=>Ur(hS(r.route.parent,t),pS(r.route,t),gS(e,r.path),mS(e,r.route))),Xt(r=>r!==!0,!0))}function pS(e,n){return e!==null&&n&&n(new il(e)),M(!0)}function hS(e,n){return e!==null&&n&&n(new nl(e)),M(!0)}function mS(e,n){let t=n.routeConfig?n.routeConfig.canActivate:null;if(!t||t.length===0)return M(!0);let r=t.map(i=>Wi(()=>{let o=n._environmentInjector,s=Oi(i,o),a=oS(s)?s.canActivate(n,e):we(o,()=>s(n,e));return Rr(a).pipe(Xt())}));return M(r).pipe(Ri())}function gS(e,n){let t=n[n.length-1],i=n.slice(0,n.length-1).reverse().map(o=>eS(o)).filter(o=>o!==null).map(o=>Wi(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Oi(a,c),d=sS(l)?l.canActivateChild(t,e):we(c,()=>l(t,e));return Rr(d).pipe(Xt())});return M(s).pipe(Ri())}));return M(i).pipe(Ri())}function vS(e,n,t,r){let i=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!i||i.length===0)return M(!0);let o=i.map(s=>{let a=n._environmentInjector,c=Oi(s,a),l=aS(c)?c.canDeactivate(e,n,t,r):we(a,()=>c(e,n,t,r));return Rr(l).pipe(Xt())});return M(o).pipe(Ri())}function yS(e,n,t,r,i){let o=n.canLoad;if(o===void 0||o.length===0)return M(!0);let s=o.map(a=>{let c=Oi(a,e),l=iS(c)?c.canLoad(n,t):we(e,()=>c(n,t)),d=Rr(l);return i?d.pipe($b(i)):d});return M(s).pipe(Ri(),zb(r))}function zb(e){return Xl(Ne(n=>{if(typeof n!="boolean")throw cl(e,n)}),L(n=>n===!0))}function bS(e,n,t,r,i,o){let s=n.canMatch;if(!s||s.length===0)return M(!0);let a=s.map(c=>{let l=Oi(c,e),d=cS(l)?l.canMatch(n,t,i):we(e,()=>l(n,t,i));return Rr(d).pipe($b(o))});return M(a).pipe(Ri(),zb(r))}var pn=class e extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,e.prototype)}},ts=class e extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,e.prototype)}};function _S(e){throw new b(4e3,!1)}function DS(e){throw Vb(!1,Be.GuardRejected)}var Lp=class{urlSerializer;urlTree;constructor(n,t){this.urlSerializer=n,this.urlTree=t}async lineralizeSegments(n,t){let r=[],i=t.root;for(;;){if(r=r.concat(i.segments),i.numberOfChildren===0)return r;if(i.numberOfChildren>1||!i.children[N])throw _S(`${n.redirectTo}`);i=i.children[N]}}async applyRedirectCommands(n,t,r,i,o){let s=await CS(t,i,o);if(s instanceof ft)throw new ts(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,r);if(s[0]==="/")throw new ts(a);return a}applyRedirectCreateUrlTree(n,t,r,i){let o=this.createSegmentGroup(n,t.root,r,i);return new ft(o,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(n,t){let r={};return Object.entries(n).forEach(([i,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);r[i]=t[a]}else r[i]=o}),r}createSegmentGroup(n,t,r,i){let o=this.createSegments(n,t.segments,r,i),s={};return Object.entries(t.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,r,i)}),new Y(o,s)}createSegments(n,t,r,i){return t.map(o=>o.path[0]===":"?this.findPosParam(n,o,i):this.findOrReturn(o,r))}findPosParam(n,t,r){let i=r[t.path.substring(1)];if(!i)throw new b(4001,!1);return i}findOrReturn(n,t){let r=0;for(let i of t){if(i.path===n.path)return t.splice(r),i;r++}return n}};function CS(e,n,t){if(typeof e=="string")return Promise.resolve(e);let r=e;return qc(Rr(we(t,()=>r(n))))}function ES(e,n){return e.providers&&!e._injector&&(e._injector=So(e.providers,n,`Route: ${e.path}`)),e._injector??n}function Nt(e){return e.outlet||N}function wS(e,n){let t=e.filter(r=>Nt(r)===n);return t.push(...e.filter(r=>Nt(r)!==n)),t}var jp={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function Gb(e){return{routeConfig:e.routeConfig,url:e.url,params:e.params,queryParams:e.queryParams,fragment:e.fragment,data:e.data,outlet:e.outlet,title:e.title,paramMap:e.paramMap,queryParamMap:e.queryParamMap}}function IS(e,n,t,r,i,o,s){let a=Wb(e,n,t);if(!a.matched)return M(a);let c=Gb(o(a));return r=ES(n,r),bS(r,n,t,i,c,s).pipe(L(l=>l===!0?a:v({},jp)))}function Wb(e,n,t){if(n.path==="")return n.pathMatch==="full"&&(e.hasChildren()||t.length>0)?v({},jp):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let i=(n.matcher||gb)(t,e,n);if(!i)return v({},jp);let o={};Object.entries(i.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=i.consumed.length>0?v(v({},o),i.consumed[i.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:i.consumed,remainingSegments:t.slice(i.consumed.length),parameters:s,positionalParamSegments:i.posParams??{}}}function hb(e,n,t,r,i){return t.length>0&&SS(e,t,r,i)?{segmentGroup:new Y(n,MS(r,new Y(t,e.children))),slicedSegments:[]}:t.length===0&&TS(e,t,r)?{segmentGroup:new Y(e.segments,xS(e,t,r,e.children)),slicedSegments:t}:{segmentGroup:new Y(e.segments,e.children),slicedSegments:t}}function xS(e,n,t,r){let i={};for(let o of t)if(fl(e,n,o)&&!r[Nt(o)]){let s=new Y([],{});i[Nt(o)]=s}return v(v({},r),i)}function MS(e,n){let t={};t[N]=n;for(let r of e)if(r.path===""&&Nt(r)!==N){let i=new Y([],{});t[Nt(r)]=i}return t}function SS(e,n,t,r){return t.some(i=>!fl(e,n,i)||!(Nt(i)!==N)?!1:!(r!==void 0&&Nt(i)===r))}function TS(e,n,t){return t.some(r=>fl(e,n,r))}function fl(e,n,t){return(e.hasChildren()||n.length>0)&&t.pathMatch==="full"?!1:t.path===""}function AS(e,n,t){return n.length===0&&!e.children[t]}var Vp=class{};async function RS(e,n,t,r,i,o,s="emptyOnly",a){return new Bp(e,n,t,r,i,s,o,a).recognize()}var NS=31,Bp=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,t,r,i,o,s,a,c){this.injector=n,this.configLoader=t,this.rootComponentType=r,this.config=i,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Lp(this.urlSerializer,this.urlTree)}noMatchError(n){return new b(4002,`'${n.segmentGroup}'`)}async recognize(){let n=hb(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:r}=await this.match(n),i=new rt(r,t),o=new Jo("",i),s=Sb(r,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let t=new Ti([],Object.freeze({}),Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),N,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,N,t),rootSnapshot:t}}catch(r){if(r instanceof ts)return this.urlTree=r.urlTree,this.match(r.urlTree.root);throw r instanceof pn?this.noMatchError(r):r}}async processSegmentGroup(n,t,r,i,o){if(r.segments.length===0&&r.hasChildren())return this.processChildren(n,t,r,o);let s=await this.processSegment(n,t,r,r.segments,i,!0,o);return s instanceof rt?[s]:[]}async processChildren(n,t,r,i){let o=[];for(let c of Object.keys(r.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=r.children[c],d=wS(t,c),u=await this.processSegmentGroup(n,d,l,c,i);s.push(...u)}let a=qb(s);return OS(a),a}async processSegment(n,t,r,i,o,s,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??n,t,c,r,i,o,s,a)}catch(l){if(l instanceof pn||Hb(l))continue;throw l}if(AS(r,i,o))return new Vp;throw new pn(r)}async processSegmentAgainstRoute(n,t,r,i,o,s,a,c){if(Nt(r)!==s&&(s===N||!fl(i,o,r)))throw new pn(i);if(r.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,i,r,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,i,t,r,o,s,c);throw new pn(i)}async expandSegmentAgainstRouteUsingRedirect(n,t,r,i,o,s,a){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:u,remainingSegments:p}=Wb(t,i,o);if(!c)throw new pn(t);typeof i.redirectTo=="string"&&i.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>NS&&(this.allowRedirects=!1));let h=this.createSnapshot(n,i,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let g=await this.applyRedirects.applyRedirectCommands(d,i.redirectTo,u,Gb(h),n),I=await this.applyRedirects.lineralizeSegments(i,g);return this.processSegment(n,r,t,I.concat(p),s,!1,a)}createSnapshot(n,t,r,i,o){let s=new Ti(r,i,Object.freeze(v({},this.urlTree.queryParams)),this.urlTree.fragment,FS(t),Nt(t),t.component??t._loadedComponent??null,t,PS(t),n),a=Hp(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,t,r,i,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Qe=>this.createSnapshot(n,r,Qe.consumedSegments,Qe.parameters,s),c=await qc(IS(t,r,i,n,this.urlSerializer,a,this.abortSignal));if(r.path==="**"&&(t.children={}),!c?.matched)throw new pn(t);n=r._injector??n;let{routes:l}=await this.getChildConfig(n,r,i),d=r._loadedInjector??n,{parameters:u,consumedSegments:p,remainingSegments:h}=c,g=this.createSnapshot(n,r,p,u,s),{segmentGroup:I,slicedSegments:D}=hb(t,p,h,l,o);if(D.length===0&&I.hasChildren()){let Qe=await this.processChildren(d,l,I,g);return new rt(g,Qe)}if(l.length===0&&D.length===0)return new rt(g,[]);let w=Nt(r)===o,ce=await this.processSegment(d,l,I,D,w?N:o,!0,g);return new rt(g,ce instanceof rt?[ce]:[])}async getChildConfig(n,t,r){if(t.children)return{routes:t.children,injector:n};if(t.loadChildren){if(t._loadedRoutes!==void 0){let o=t._loadedNgModuleFactory;return o&&!t._loadedInjector&&(t._loadedInjector=o.create(n).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await qc(yS(n,t,r,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,t);return t._loadedRoutes=o.routes,t._loadedInjector=o.injector,t._loadedNgModuleFactory=o.factory,o}throw DS(t)}return{routes:[],injector:n}}};function OS(e){e.sort((n,t)=>n.value.outlet===N?-1:t.value.outlet===N?1:n.value.outlet.localeCompare(t.value.outlet))}function kS(e){let n=e.value.routeConfig;return n&&n.path===""}function qb(e){let n=[],t=new Set;for(let r of e){if(!kS(r)){n.push(r);continue}let i=n.find(o=>r.value.routeConfig===o.value.routeConfig);i!==void 0?(i.children.push(...r.children),t.add(i)):n.push(r)}for(let r of t){let i=qb(r.children);n.push(new rt(r.value,i))}return n.filter(r=>!t.has(r))}function FS(e){return e.data||{}}function PS(e){return e.resolve||{}}function LS(e,n,t,r,i,o,s){return $e(async a=>{let{state:c,tree:l}=await RS(e,n,t,r,a.extractedUrl,i,o,s);return P(v({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function jS(e){return $e(n=>{let{targetSnapshot:t,guards:{canActivateChecks:r}}=n;if(!r.length)return M(n);let i=new Set(r.map(a=>a.route)),o=new Set;for(let a of i)if(!o.has(a))for(let c of Zb(a))o.add(c);let s=0;return de(o).pipe($r(a=>i.has(a)?VS(a,t,e):(a.data=Hp(a,a.parent,e).resolve,M(void 0))),Ne(()=>s++),ra(1),$e(a=>s===o.size?M(n):De))})}function Zb(e){let n=e.children.map(t=>Zb(t)).flat();return[e,...n]}function VS(e,n,t){let r=e.routeConfig,i=e._resolve;return r?.title!==void 0&&!Pb(r)&&(i[ns]=r.title),Wi(()=>(e.data=Hp(e,e.parent,t).resolve,BS(i,e,n).pipe(L(o=>(e._resolvedData=o,e.data=v(v({},e.data),o),null)))))}function BS(e,n,t){let r=Mp(e);if(r.length===0)return M({});let i={};return de(r).pipe($e(o=>HS(e[o],n,t).pipe(Xt(),Ne(s=>{if(s instanceof Ai)throw cl(new Vn,s);i[o]=s}))),ra(1),L(()=>i),Pt(o=>Hb(o)?De:Jn(o)))}function HS(e,n,t){let r=n._environmentInjector,i=Oi(e,r),o=i.resolve?i.resolve(n,t):we(r,()=>i(n,t));return Rr(o)}function mb(e){return Je(n=>{let t=e(n);return t?de(t).pipe(L(()=>n)):M(n)})}var Gp=(()=>{class e{buildTitle(t){let r,i=t.root;for(;i!==void 0;)r=this.getResolvedTitleForRoute(i)??r,i=i.children.find(o=>o.outlet===N);return r}getResolvedTitleForRoute(t){return t.data[ns]}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>f(Qb),providedIn:"root"})}return e})(),Qb=(()=>{class e extends Gp{title;constructor(t){super(),this.title=t}updateTitle(t){let r=this.buildTitle(t);r!==void 0&&this.title.setTitle(r)}static \u0275fac=function(r){return new(r||e)(E(ab))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),ss=new m("",{factory:()=>({})}),as=new m(""),Yb=(()=>{class e{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=f(Xf);async loadComponent(t,r){if(this.componentLoaders.get(r))return this.componentLoaders.get(r);if(r._loadedComponent)return Promise.resolve(r._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(r);let i=(async()=>{try{let o=await yb(we(t,()=>r.loadComponent())),s=await Jb(Xb(o));return this.onLoadEndListener&&this.onLoadEndListener(r),r._loadedComponent=s,s}finally{this.componentLoaders.delete(r)}})();return this.componentLoaders.set(r,i),i}loadChildren(t,r){if(this.childrenLoaders.get(r))return this.childrenLoaders.get(r);if(r._loadedRoutes)return Promise.resolve({routes:r._loadedRoutes,injector:r._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(r);let i=(async()=>{try{let o=await Kb(r,this.compiler,t,this.onLoadEndListener);return r._loadedRoutes=o.routes,r._loadedInjector=o.injector,r._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(r)}})();return this.childrenLoaders.set(r,i),i}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();async function Kb(e,n,t,r){let i=await yb(we(t,()=>e.loadChildren())),o=await Jb(Xb(i)),s;o instanceof hc||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),r&&r(e);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(t).injector,d=s,c=a.get(as,[],{optional:!0,self:!0}).flat()),{routes:c.map(zp),injector:a,factory:d}}function US(e){return e&&typeof e=="object"&&"default"in e}function Xb(e){return US(e)?e.default:e}async function Jb(e){return e}var pl=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>f($S),providedIn:"root"})}return e})(),$S=(()=>{class e{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,r){return t}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),e_=new m("");var zS=()=>{},t_=new m(""),n_=(()=>{class e{currentNavigation=We(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=We(null);events=new H;transitionAbortWithErrorSubject=new H;configLoader=f(Yb);environmentInjector=f(ue);destroyRef=f(Dt);urlSerializer=f(rs);rootContexts=f(Ni);location=f(yi);inputBindingEnabled=f(ul,{optional:!0})!==null;titleStrategy=f(Gp);options=f(ss,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=f(pl);createViewTransition=f(e_,{optional:!0});navigationErrorHandler=f(t_,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>M(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=i=>this.events.next(new el(i)),r=i=>this.events.next(new tl(i));this.configLoader.onLoadEndListener=r,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let r=++this.navigationId;je(()=>{this.transitions?.next(P(v({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:r,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new _e(null),this.transitions.pipe(Se(r=>r!==null),Je(r=>{let i=!1,o=new AbortController,s=()=>!i&&this.currentTransition?.id===r.id;return M(r).pipe(Je(a=>{if(this.navigationId>r.id)return this.cancelNavigationTransition(r,"",Be.SupersededByNewNavigation),De;this.currentTransition=r;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?P(v({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),d=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&d!=="reload")return this.events.next(new mn(a.id,this.urlSerializer.serialize(a.rawUrl),"",Qo.IgnoredSameUrlNavigation)),a.resolve(!1),De;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return M(a).pipe(Je(u=>(this.events.next(new Tr(u.id,this.urlSerializer.serialize(u.extractedUrl),u.source,u.restoredState)),u.id!==this.navigationId?De:Promise.resolve(u))),LS(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),Ne(u=>{r.targetSnapshot=u.targetSnapshot,r.urlAfterRedirects=u.urlAfterRedirects,this.currentNavigation.update(p=>(p.finalUrl=u.urlAfterRedirects,p)),this.events.next(new Ko)}),Je(u=>de(r.routesRecognizeHandler.deferredHandle??M(void 0)).pipe(L(()=>u))),Ne(()=>{let u=new Yo(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(u)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:u,extractedUrl:p,source:h,restoredState:g,extras:I}=a,D=new Tr(u,this.urlSerializer.serialize(p),h,g);this.events.next(D);let w=kb(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=r=P(v({},a),{targetSnapshot:w,urlAfterRedirects:p,extras:P(v({},I),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(ce=>(ce.finalUrl=p,ce)),M(r)}else return this.events.next(new mn(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Qo.IgnoredByUrlHandlingStrategy)),a.resolve(!1),De}),L(a=>{let c=new Yc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=r=P(v({},a),{guards:JM(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),r}),dS(a=>this.events.next(a)),Je(a=>{if(r.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw cl(this.urlSerializer,a.guardsResult);let c=new Kc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return De;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",Be.GuardRejected),De;if(a.guards.canActivateChecks.length===0)return M(a);let l=new Xc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return De;let d=!1;return M(a).pipe(jS(this.paramsInheritanceStrategy),Ne({next:()=>{d=!0;let u=new Jc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(u)},complete:()=>{d||this.cancelNavigationTransition(a,"",Be.NoDataFromResolver)}}))}),mb(a=>{let c=d=>{let u=[];if(d.routeConfig?._loadedComponent)d.component=d.routeConfig?._loadedComponent;else if(d.routeConfig?.loadComponent){let p=d._environmentInjector;u.push(this.configLoader.loadComponent(p,d.routeConfig).then(h=>{d.component=h}))}for(let p of d.children)u.push(...c(p));return u},l=c(a.targetSnapshot.root);return l.length===0?M(a):de(Promise.all(l).then(()=>a))}),mb(()=>this.afterPreactivation()),Je(()=>{let{currentSnapshot:a,targetSnapshot:c}=r,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?de(l).pipe(L(()=>r)):M(r)}),mt(1),Je(a=>{let c=QM(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=r=a=P(v({},a),{targetRouterState:c}),this.currentNavigation.update(d=>(d.targetRouterState=c,d)),this.events.next(new Mi);let l=r.beforeActivateHandler.deferredHandle;return l?de(l.then(()=>a)):M(a)}),Ne(a=>{new Pp(t.routeReuseStrategy,r.targetRouterState,r.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(i=!0,this.currentNavigation.update(c=>(c.abort=zS,c)),this.lastSuccessfulNavigation.set(je(this.currentNavigation)),this.events.next(new hn(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),gt(Ub(o.signal).pipe(Se(()=>!i&&!r.targetRouterState),Ne(()=>{this.cancelNavigationTransition(r,o.signal.reason+"",Be.Aborted)}))),Ne({complete:()=>{i=!0}}),gt(this.transitionAbortWithErrorSubject.pipe(Ne(a=>{throw a}))),tr(()=>{o.abort(),i||this.cancelNavigationTransition(r,"",Be.SupersededByNewNavigation),this.currentTransition?.id===r.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Pt(a=>{if(i=!0,this.destroyed)return r.resolve(!1),De;if(Bb(a))this.events.next(new ut(r.id,this.urlSerializer.serialize(r.extractedUrl),a.message,a.cancellationCode)),XM(a)?this.events.next(new Si(a.url,a.navigationBehaviorOptions)):r.resolve(!1);else{let c=new Ar(r.id,this.urlSerializer.serialize(r.extractedUrl),a,r.targetSnapshot??void 0);try{let l=we(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof Ai){let{message:d,cancellationCode:u}=cl(this.urlSerializer,l);this.events.next(new ut(r.id,this.urlSerializer.serialize(r.extractedUrl),d,u)),this.events.next(new Si(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?r.resolve(!1):r.reject(l)}}return De}))}))}cancelNavigationTransition(t,r,i){let o=new ut(t.id,this.urlSerializer.serialize(t.extractedUrl),r,i);this.events.next(o),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),r=je(this.currentNavigation),i=r?.targetBrowserUrl??r?.extractedUrl;return t.toString()!==i?.toString()&&!r?.extras.skipLocationChange}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function GS(e){return e!==Wo}var r_=new m("");var i_=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>f(WS),providedIn:"root"})}return e})(),dl=class{shouldDetach(n){return!1}store(n,t){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,t){return n.routeConfig===t.routeConfig}shouldDestroyInjector(n){return!0}},WS=(()=>{class e extends dl{static \u0275fac=(()=>{let t;return function(i){return(t||(t=an(e)))(i||e)}})();static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),Wp=(()=>{class e{urlSerializer=f(rs);options=f(ss,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=f(yi);urlHandlingStrategy=f(pl);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new ft;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:r,targetBrowserUrl:i}){let o=t!==void 0?this.urlHandlingStrategy.merge(t,r):r,s=i??o;return s instanceof ft?this.urlSerializer.serialize(s):s}commitTransition({targetRouterState:t,finalUrl:r,initialUrl:i}){r&&t?(this.currentUrlTree=r,this.rawUrlTree=this.urlHandlingStrategy.merge(r,i),this.routerState=t):this.rawUrlTree=i}routerState=kb(null,f(ue));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:()=>f(qS),providedIn:"root"})}return e})(),qS=(()=>{class e extends Wp{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(r=>{r.type==="popstate"&&setTimeout(()=>{t(r.url,r.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,r){t instanceof Tr?this.updateStateMemento():t instanceof mn?this.commitTransition(r):t instanceof Yo?this.urlUpdateStrategy==="eager"&&(r.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(r),r)):t instanceof Mi?(this.commitTransition(r),this.urlUpdateStrategy==="deferred"&&!r.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(r),r)):t instanceof ut&&!Ob(t)?this.restoreHistory(r):t instanceof Ar?this.restoreHistory(r,!0):t instanceof hn&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,{extras:r,id:i}){let{replaceUrl:o,state:s}=r;if(this.location.isCurrentPathEqualTo(t)||o){let a=this.browserPageId,c=v(v({},s),this.generateNgRouterState(i,a));this.location.replaceState(t,"",c)}else{let a=v(v({},s),this.generateNgRouterState(i,this.browserPageId+1));this.location.go(t,"",a)}}restoreHistory(t,r=!1){if(this.canceledNavigationResolution==="computed"){let i=this.browserPageId,o=this.currentPageId-i;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===t.finalUrl&&o===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(r&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,r){return this.canceledNavigationResolution==="computed"?{navigationId:t,\u0275routerPageId:r}:{navigationId:t}}static \u0275fac=(()=>{let t;return function(i){return(t||(t=an(e)))(i||e)}})();static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function qp(e,n){e.events.pipe(Se(t=>t instanceof hn||t instanceof ut||t instanceof Ar||t instanceof mn),L(t=>t instanceof hn||t instanceof mn?0:(t instanceof ut?t.code===Be.Redirect||t.code===Be.SupersededByNewNavigation:!1)?2:1),Se(t=>t!==2),mt(1)).subscribe(()=>{n()})}var hl=(()=>{class e{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=f(mc);stateManager=f(Wp);options=f(ss,{optional:!0})||{};pendingTasks=f(rn);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=f(n_);urlSerializer=f(rs);location=f(yi);urlHandlingStrategy=f(pl);injector=f(ue);_events=new H;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=f(i_);injectorCleanup=f(r_,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=f(as,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!f(ul,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new pe;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(r=>{try{let i=this.navigationTransitions.currentTransition,o=je(this.navigationTransitions.currentNavigation);if(i!==null&&o!==null){if(this.stateManager.handleRouterEvent(r,o),r instanceof ut&&r.code!==Be.Redirect&&r.code!==Be.SupersededByNewNavigation)this.navigated=!0;else if(r instanceof hn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(r instanceof Si){let s=r.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(r.url,i.currentRawUrl),c=v({scroll:i.extras.scroll,browserUrl:i.extras.browserUrl,info:i.extras.info,skipLocationChange:i.extras.skipLocationChange,replaceUrl:i.extras.replaceUrl||this.urlUpdateStrategy==="eager"||GS(i.source)},s);this.scheduleNavigation(a,Wo,null,c,{resolve:i.resolve,reject:i.reject,promise:i.promise})}}qM(r)&&this._events.next(r)}catch(i){this.navigationTransitions.transitionAbortWithErrorSubject.next(i)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Wo,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,r,i,o)=>{this.navigateToSyncWithBrowser(t,i,r,o)})}navigateToSyncWithBrowser(t,r,i,o){let s=i?.navigationId?i:null;if(i){let c=v({},i);delete c.navigationId,delete c.\u0275routerPageId,Object.keys(c).length!==0&&(o.state=c)}let a=this.parseUrl(t);this.scheduleNavigation(a,r,s,o).catch(c=>{this.disposed||this.injector.get(Ct)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return je(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(zp),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,r={}){let{relativeTo:i,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=r,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=v(v({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let u;try{let p=i?i.snapshot:this.routerState.snapshot.root;u=Tb(p)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),u=this.currentUrlTree.root}return Ab(u,t,d,l??null,this.urlSerializer)}navigateByUrl(t,r={skipLocationChange:!1}){let i=xi(t)?t:this.parseUrl(t),o=this.urlHandlingStrategy.merge(i,this.rawUrlTree);return this.scheduleNavigation(o,Wo,null,r)}navigate(t,r={skipLocationChange:!1}){return ZS(t),this.navigateByUrl(this.createUrlTree(t,r),r)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(Cn(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,r){let i;if(r===!0?i=v({},_b):r===!1?i=v({},Sp):i=v(v({},Sp),r),xi(t))return lb(this.currentUrlTree,t,i);let o=this.parseUrl(t);return lb(this.currentUrlTree,o,i)}removeEmptyProps(t){return Object.entries(t).reduce((r,[i,o])=>(o!=null&&(r[i]=o),r),{})}scheduleNavigation(t,r,i,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((u,p)=>{a=u,c=p});let d=this.pendingTasks.add();return qp(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:r,restoredState:i,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function ZS(e){for(let n=0;n<e.length;n++)if(e[n]==null)throw new b(4008,!1)}var KS=new m("");function Zp(e,...n){return cr([{provide:as,multi:!0,useValue:e},[],{provide:Bn,useFactory:XS},{provide:gc,multi:!0,useFactory:JS},n.map(t=>t.\u0275providers)])}function XS(){return f(hl).routerState.root}function JS(){let e=f(he);return n=>{let t=e.get(qt);if(n!==t.components[0])return;let r=e.get(hl),i=e.get(eT);e.get(tT)===1&&r.initialNavigation(),e.get(nT,null,{optional:!0})?.setUpPreloading(),e.get(KS,null,{optional:!0})?.init(),r.resetRootComponentType(t.componentTypes[0]),i.closed||(i.next(),i.complete(),i.unsubscribe())}}var eT=new m("",{factory:()=>new H}),tT=new m("",{factory:()=>1});var nT=new m("");var o_=[];var s_={providers:[Zp(o_),_p()]};var h_=(()=>{class e{_renderer;_elementRef;onChange=t=>{};onTouched=()=>{};constructor(t,r){this._renderer=t,this._elementRef=r}setProperty(t,r){this._renderer.setProperty(this._elementRef.nativeElement,t,r)}registerOnTouched(t){this.onTouched=t}registerOnChange(t){this.onChange=t}setDisabledState(t){this.setProperty("disabled",t)}static \u0275fac=function(r){return new(r||e)(oe(St),oe(J))};static \u0275dir=Q({type:e})}return e})(),iT=(()=>{class e extends h_{static \u0275fac=(()=>{let t;return function(i){return(t||(t=an(e)))(i||e)}})();static \u0275dir=Q({type:e,features:[lt]})}return e})(),m_=new m("");var oT={provide:m_,useExisting:En(()=>bl),multi:!0};function sT(){let e=dt()?dt().getUserAgent():"";return/android (\d+)/.test(e.toLowerCase())}var aT=new m(""),bl=(()=>{class e extends h_{_compositionMode;_composing=!1;constructor(t,r,i){super(t,r),this._compositionMode=i,this._compositionMode==null&&(this._compositionMode=!sT())}writeValue(t){let r=t??"";this.setProperty("value",r)}_handleInput(t){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(t)}_compositionStart(){this._composing=!0}_compositionEnd(t){this._composing=!1,this._compositionMode&&this.onChange(t)}static \u0275fac=function(r){return new(r||e)(oe(St),oe(J),oe(aT,8))};static \u0275dir=Q({type:e,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(r,i){r&1&&Ze("input",function(s){return i._handleInput(s.target.value)})("blur",function(){return i.onTouched()})("compositionstart",function(){return i._compositionStart()})("compositionend",function(s){return i._compositionEnd(s.target.value)})},standalone:!1,features:[Qt([oT]),lt]})}return e})();var cT=new m(""),lT=new m("");function g_(e){return e!=null}function v_(e){return kn(e)?de(e):e}function y_(e){let n={};return e.forEach(t=>{n=t!=null?v(v({},n),t):n}),Object.keys(n).length===0?null:n}function b_(e,n){return n.map(t=>t(e))}function dT(e){return!e.validate}function __(e){return e.map(n=>dT(n)?n:t=>n.validate(t))}function uT(e){if(!e)return null;let n=e.filter(g_);return n.length==0?null:function(t){return y_(b_(t,n))}}function D_(e){return e!=null?uT(__(e)):null}function fT(e){if(!e)return null;let n=e.filter(g_);return n.length==0?null:function(t){let r=b_(t,n).map(v_);return qi(r).pipe(L(y_))}}function C_(e){return e!=null?fT(__(e)):null}function a_(e,n){return e===null?[n]:Array.isArray(e)?[...e,n]:[e,n]}function pT(e){return e._rawValidators}function hT(e){return e._rawAsyncValidators}function Qp(e){return e?Array.isArray(e)?e:[e]:[]}function gl(e,n){return Array.isArray(e)?e.includes(n):e===n}function c_(e,n){let t=Qp(n);return Qp(e).forEach(i=>{gl(t,i)||t.push(i)}),t}function l_(e,n){return Qp(n).filter(t=>!gl(e,t))}var vl=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=D_(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=C_(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,t){return this.control?this.control.hasError(n,t):!1}getError(n,t){return this.control?this.control.getError(n,t):null}},Yp=class extends vl{name;get formDirective(){return null}get path(){return null}},fs=class extends vl{_parent=null;name=null;valueAccessor=null},Kp=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var E_=(()=>{class e extends Kp{constructor(t){super(t)}static \u0275fac=function(r){return new(r||e)(oe(fs,2))};static \u0275dir=Q({type:e,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(r,i){r&2&&se("ng-untouched",i.isUntouched)("ng-touched",i.isTouched)("ng-pristine",i.isPristine)("ng-dirty",i.isDirty)("ng-valid",i.isValid)("ng-invalid",i.isInvalid)("ng-pending",i.isPending)},standalone:!1,features:[lt]})}return e})();var cs="VALID",ml="INVALID",ki="PENDING",ls="DISABLED",Nr=class{},yl=class extends Nr{value;source;constructor(n,t){super(),this.value=n,this.source=t}},ds=class extends Nr{pristine;source;constructor(n,t){super(),this.pristine=n,this.source=t}},us=class extends Nr{touched;source;constructor(n,t){super(),this.touched=n,this.source=t}},Fi=class extends Nr{status;source;constructor(n,t){super(),this.status=n,this.source=t}};var Xp=class extends Nr{source;constructor(n){super(),this.source=n}};function mT(e){return(_l(e)?e.validators:e)||null}function gT(e){return Array.isArray(e)?D_(e):e||null}function vT(e,n){return(_l(n)?n.asyncValidators:e)||null}function yT(e){return Array.isArray(e)?C_(e):e||null}function _l(e){return e!=null&&!Array.isArray(e)&&typeof e=="object"}var Jp=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,t){this._assignValidators(n),this._assignAsyncValidators(t)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return je(this.statusReactive)}set status(n){je(()=>this.statusReactive.set(n))}_status=At(()=>this.statusReactive());statusReactive=We(void 0);get valid(){return this.status===cs}get invalid(){return this.status===ml}get pending(){return this.status===ki}get disabled(){return this.status===ls}get enabled(){return this.status!==ls}errors;get pristine(){return je(this.pristineReactive)}set pristine(n){je(()=>this.pristineReactive.set(n))}_pristine=At(()=>this.pristineReactive());pristineReactive=We(!0);get dirty(){return!this.pristine}get touched(){return je(this.touchedReactive)}set touched(n){je(()=>this.touchedReactive.set(n))}_touched=At(()=>this.touchedReactive());touchedReactive=We(!1);get untouched(){return!this.touched}_events=new H;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(c_(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(c_(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(l_(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(l_(n,this._rawAsyncValidators))}hasValidator(n){return gl(this._rawValidators,n)}hasAsyncValidator(n){return gl(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let t=this.touched===!1;this.touched=!0;let r=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(P(v({},n),{sourceControl:r})),t&&n.emitEvent!==!1&&this._events.next(new us(!0,r))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(t=>t.markAllAsTouched(n))}markAsUntouched(n={}){let t=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let r=n.sourceControl??this;this._forEachChild(i=>{i.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:r})}),n.onlySelf||this._parent?._updateTouched(n,r),t&&n.emitEvent!==!1&&this._events.next(new us(!1,r))}markAsDirty(n={}){let t=this.pristine===!0;this.pristine=!1;let r=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(P(v({},n),{sourceControl:r})),t&&n.emitEvent!==!1&&this._events.next(new ds(!1,r))}markAsPristine(n={}){let t=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let r=n.sourceControl??this;this._forEachChild(i=>{i.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,r),t&&n.emitEvent!==!1&&this._events.next(new ds(!0,r))}markAsPending(n={}){this.status=ki;let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Fi(this.status,t)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(P(v({},n),{sourceControl:t}))}disable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=ls,this.errors=null,this._forEachChild(i=>{i.disable(P(v({},n),{onlySelf:!0}))}),this._updateValue();let r=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new yl(this.value,r)),this._events.next(new Fi(this.status,r)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(P(v({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(i=>i(!0))}enable(n={}){let t=this._parentMarkedDirty(n.onlySelf);this.status=cs,this._forEachChild(r=>{r.enable(P(v({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(P(v({},n),{skipPristineCheck:t}),this),this._onDisabledChange.forEach(r=>r(!1))}_updateAncestors(n,t){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},t),this._parent?._updateTouched({},t))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let r=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===cs||this.status===ki)&&this._runAsyncValidator(r,n.emitEvent)}let t=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new yl(this.value,t)),this._events.next(new Fi(this.status,t)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(P(v({},n),{sourceControl:t}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(t=>t._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?ls:cs}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,t){if(this.asyncValidator){this.status=ki,this._hasOwnPendingAsyncValidator={emitEvent:t!==!1,shouldHaveEmitted:n!==!1};let r=v_(this.asyncValidator(this));this._asyncValidationSubscription=r.subscribe(i=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(i,{emitEvent:t,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,t={}){this.errors=n,this._updateControlsErrors(t.emitEvent!==!1,this,t.shouldHaveEmitted)}get(n){let t=n;return t==null||(Array.isArray(t)||(t=t.split(".")),t.length===0)?null:t.reduce((r,i)=>r&&r._find(i),this)}getError(n,t){let r=t?this.get(t):this;return r?.errors?r.errors[n]:null}hasError(n,t){return!!this.getError(n,t)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,t,r){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||r)&&this._events.next(new Fi(this.status,t)),this._parent&&this._parent._updateControlsErrors(n,t,r)}_initObservables(){this.valueChanges=new re,this.statusChanges=new re}_calculateStatus(){return this._allControlsDisabled()?ls:this.errors?ml:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ki)?ki:this._anyControlsHaveStatus(ml)?ml:cs}_anyControlsHaveStatus(n){return this._anyControls(t=>t.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,t){let r=!this._anyControlsDirty(),i=this.pristine!==r;this.pristine=r,n.onlySelf||this._parent?._updatePristine(n,t),i&&this._events.next(new ds(this.pristine,t))}_updateTouched(n={},t){this.touched=this._anyControlsTouched(),this._events.next(new us(this.touched,t)),n.onlySelf||this._parent?._updateTouched(n,t)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){_l(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=gT(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=yT(this._rawAsyncValidators)}};var w_=new m("",{factory:()=>eh}),eh="always";function bT(e,n){return[...n.path,e]}function _T(e,n,t=eh){CT(e,n),n.valueAccessor.writeValue(e.value),(e.disabled||t==="always")&&n.valueAccessor.setDisabledState?.(e.disabled),ET(e,n),IT(e,n),wT(e,n),DT(e,n)}function d_(e,n){e.forEach(t=>{t.registerOnValidatorChange&&t.registerOnValidatorChange(n)})}function DT(e,n){if(n.valueAccessor.setDisabledState){let t=r=>{n.valueAccessor.setDisabledState(r)};e.registerOnDisabledChange(t),n._registerOnDestroy(()=>{e._unregisterOnDisabledChange(t)})}}function CT(e,n){let t=pT(e);n.validator!==null?e.setValidators(a_(t,n.validator)):typeof t=="function"&&e.setValidators([t]);let r=hT(e);n.asyncValidator!==null?e.setAsyncValidators(a_(r,n.asyncValidator)):typeof r=="function"&&e.setAsyncValidators([r]);let i=()=>e.updateValueAndValidity();d_(n._rawValidators,i),d_(n._rawAsyncValidators,i)}function ET(e,n){n.valueAccessor.registerOnChange(t=>{e._pendingValue=t,e._pendingChange=!0,e._pendingDirty=!0,e.updateOn==="change"&&I_(e,n)})}function wT(e,n){n.valueAccessor.registerOnTouched(()=>{e._pendingTouched=!0,e.updateOn==="blur"&&e._pendingChange&&I_(e,n),e.updateOn!=="submit"&&e.markAsTouched()})}function I_(e,n){e._pendingDirty&&e.markAsDirty(),e.setValue(e._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(e._pendingValue),e._pendingChange=!1}function IT(e,n){let t=(r,i)=>{n.valueAccessor.writeValue(r),i&&n.viewToModelUpdate(r)};e.registerOnChange(t),n._registerOnDestroy(()=>{e._unregisterOnChange(t)})}function xT(e,n){if(!e.hasOwnProperty("model"))return!1;let t=e.model;return t.isFirstChange()?!0:!Object.is(n,t.currentValue)}function MT(e){return Object.getPrototypeOf(e.constructor)===iT}function ST(e,n){if(!n)return null;Array.isArray(n);let t,r,i;return n.forEach(o=>{o.constructor===bl?t=o:MT(o)?r=o:i=o}),i||r||t||null}function u_(e,n){let t=e.indexOf(n);t>-1&&e.splice(t,1)}function f_(e){return typeof e=="object"&&e!==null&&Object.keys(e).length===2&&"value"in e&&"disabled"in e}var TT=class extends Jp{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,t,r){super(mT(t),vT(r,t)),this._applyFormState(n),this._setUpdateStrategy(t),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),_l(t)&&(t.nonNullable||t.initialValueIsDefault)&&(f_(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,t={}){this.value=this._pendingValue=n,this._onChange.length&&t.emitModelToViewChange!==!1&&this._onChange.forEach(r=>r(this.value,t.emitViewToModelChange!==!1)),this.updateValueAndValidity(t)}patchValue(n,t={}){this.setValue(n,t)}reset(n=this.defaultValue,t={}){this._applyFormState(n),this.markAsPristine(t),this.markAsUntouched(t),this.setValue(this.value,t),t.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,t?.emitEvent!==!1&&this._events.next(new Xp(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){u_(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){u_(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){f_(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var AT={provide:fs,useExisting:En(()=>th)},p_=Promise.resolve(),th=(()=>{class e extends fs{_changeDetectorRef;callSetDisabledState;control=new TT;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new re;constructor(t,r,i,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=t,this._setValidators(r),this._setAsyncValidators(i),this.valueAccessor=ST(this,o)}ngOnChanges(t){if(this._checkForErrors(),!this._registered||"name"in t){if(this._registered&&(this._checkName(),this.formDirective)){let r=t.name.previousValue;this.formDirective.removeControl({name:r,path:this._getPath(r)})}this._setUpControl()}"isDisabled"in t&&this._updateDisabled(t),xT(t,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(t){this.viewModel=t,this.update.emit(t)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){_T(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(t){p_.then(()=>{this.control.setValue(t,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(t){let r=t.isDisabled.currentValue,i=r!==0&&Ve(r);p_.then(()=>{i&&!this.control.disabled?this.control.disable():!i&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(t){return this._parent?bT(t,this._parent):[t]}static \u0275fac=function(r){return new(r||e)(oe(Yp,9),oe(cT,10),oe(lT,10),oe(m_,10),oe(Rt,8),oe(w_,8))};static \u0275dir=Q({type:e,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Qt([AT]),lt,sn]})}return e})();var RT=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({})}return e})();var x_=(()=>{class e{static withConfig(t){return{ngModule:e,providers:[{provide:w_,useValue:t.callSetDisabledState??eh}]}}static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[RT]})}return e})();var Dl=class e{message="An error occurred.";retry=new re;onRetry(){this.retry.emit()}static \u0275fac=function(t){return new(t||e)};static \u0275cmp=W({type:e,selectors:[["app-error-message"]],inputs:{message:"message"},outputs:{retry:"retry"},decls:7,vars:1,consts:[[1,"error-container"],[1,"error-icon"],[1,"error-text"],[1,"retry-btn",3,"click"]],template:function(t,r){t&1&&(Pe(0,"div",0)(1,"div",1),q(2,"\u274C"),qe(),Pe(3,"p",2),q(4),qe(),Pe(5,"button",3),vc("click",function(){return r.onRetry()}),q(6,"Try Again"),qe()()),t&2&&(S(4),nt(r.message))},styles:[".error-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:12px;padding:32px 20px;text-align:center}.error-container[_ngcontent-%COMP%]   .error-icon[_ngcontent-%COMP%]{font-size:2rem}.error-container[_ngcontent-%COMP%]   .error-text[_ngcontent-%COMP%]{color:#ef4444;font-weight:500}.error-container[_ngcontent-%COMP%]   .retry-btn[_ngcontent-%COMP%]{padding:10px 24px;background:#ef4444;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:.9rem;font-weight:600}.error-container[_ngcontent-%COMP%]   .retry-btn[_ngcontent-%COMP%]:hover{opacity:.85}"]})};var Cl=class e{message="Loading...";static \u0275fac=function(t){return new(t||e)};static \u0275cmp=W({type:e,selectors:[["app-loading-spinner"]],inputs:{message:"message"},decls:4,vars:1,consts:[[1,"spinner-container"],[1,"spinner"],[1,"spinner-text"]],template:function(t,r){t&1&&(Pe(0,"div",0),Tt(1,"div",1),Pe(2,"p",2),q(3),qe()()),t&2&&(S(3),nt(r.message))},styles:[".spinner-container[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;gap:16px;padding:40px 20px}.spinner-container[_ngcontent-%COMP%]   .spinner[_ngcontent-%COMP%]{width:48px;height:48px;border:4px solid rgba(102,126,234,.2);border-top-color:#667eea;border-radius:50%;animation:_ngcontent-%COMP%_spin .8s linear infinite}.spinner-container[_ngcontent-%COMP%]   .spinner-text[_ngcontent-%COMP%]{color:#9ca3af;font-size:.95rem}@keyframes _ngcontent-%COMP%_spin{to{transform:rotate(360deg)}}"]})};var El=class e{transform(n,t="km/h"){return n<15?`${n} ${t} (Calm)`:n<30?`${n} ${t} (Moderate)`:n<70?`${n} ${t} (Strong)`:`${n} ${t} (Very Strong)`}static \u0275fac=function(t){return new(t||e)};static \u0275pipe=To({name:"windSpeed",type:e,pure:!0})};function Hn(e){return e instanceof J?e.nativeElement:e}function M_(e){return e!=null&&`${e}`!="false"}var nh;try{nh=typeof Intl<"u"&&Intl.v8BreakIterator}catch{nh=!1}var Ot=(()=>{class e{_platformId=f(Cr);isBrowser=this._platformId?By(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||nh)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var rh;function S_(){if(rh==null){let e=typeof document<"u"?document.head:null;rh=!!(e&&(e.createShadowRoot||e.attachShadow))}return rh}function ih(e){if(S_()){let n=e.getRootNode?e.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function kt(e){return e.composedPath?e.composedPath()[0]:e.target}var ps;function T_(){if(ps==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>ps=!0}))}finally{ps=ps||!1}return ps}function Pi(e){return T_()?e:!!e.capture}var wl=new WeakMap,Un=(()=>{class e{_appRef;_injector=f(he);_environmentInjector=f(ue);load(t){let r=this._appRef=this._appRef||this._injector.get(qt),i=wl.get(r);i||(i={loaders:new Set,refs:[]},wl.set(r,i),r.onDestroy(()=>{wl.get(r)?.refs.forEach(o=>o.destroy()),wl.delete(r)})),i.loaders.has(t)||(i.loaders.add(t),i.refs.push(Ny(t,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var A_=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({})}return e})();function hs(e){return e.buttons===0||e.detail===0}function ms(e){let n=e.touches&&e.touches[0]||e.changedTouches&&e.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var R_=new m("cdk-input-modality-detector-options"),N_={ignoreKeys:[18,17,224,91,16]},O_=650,oh={passive:!0,capture:!0},k_=(()=>{class e{_platform=f(Ot);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new _e(null);_options;_lastTouchMs=0;_onKeydown=t=>{this._options?.ignoreKeys?.some(r=>r===t.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=kt(t))};_onMousedown=t=>{Date.now()-this._lastTouchMs<O_||(this._modality.next(hs(t)?"keyboard":"mouse"),this._mostRecentTarget=kt(t))};_onTouchstart=t=>{if(ms(t)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=kt(t)};constructor(){let t=f($),r=f(U),i=f(R_,{optional:!0});if(this._options=v(v({},N_),i),this.modalityDetected=this._modality.pipe(cd(1)),this.modalityChanged=this.modalityDetected.pipe(rd()),this._platform.isBrowser){let o=f(xt).createRenderer(null,null);this._listenerCleanups=t.runOutsideAngular(()=>[o.listen(r,"keydown",this._onKeydown,oh),o.listen(r,"mousedown",this._onMousedown,oh),o.listen(r,"touchstart",this._onTouchstart,oh)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(t=>t())}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})(),gs=(function(e){return e[e.IMMEDIATE=0]="IMMEDIATE",e[e.EVENTUAL=1]="EVENTUAL",e})(gs||{}),F_=new m("cdk-focus-monitor-default-options"),Il=Pi({passive:!0,capture:!0}),vs=(()=>{class e{_ngZone=f($);_platform=f(Ot);_inputModalityDetector=f(k_);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=f(U);_stopInputModalityDetector=new H;constructor(){let t=f(F_,{optional:!0});this._detectionMode=t?.detectionMode||gs.IMMEDIATE}_rootNodeFocusAndBlurListener=t=>{let r=kt(t);for(let i=r;i;i=i.parentElement)t.type==="focus"?this._onFocus(t,i):this._onBlur(t,i)};monitor(t,r=!1){let i=Hn(t);if(!this._platform.isBrowser||i.nodeType!==1)return M();let o=ih(i)||this._document,s=this._elementInfo.get(i);if(s)return r&&(s.checkChildren=!0),s.subject;let a={checkChildren:r,subject:new H,rootNode:o};return this._elementInfo.set(i,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(t){let r=Hn(t),i=this._elementInfo.get(r);i&&(i.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(i))}focusVia(t,r,i){let o=Hn(t),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,r,c)):(this._setOrigin(r),typeof o.focus=="function"&&o.focus(i))}ngOnDestroy(){this._elementInfo.forEach((t,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(t){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(t)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:t&&this._isLastInteractionFromInputLabel(t)?"mouse":"program"}_shouldBeAttributedToTouch(t){return this._detectionMode===gs.EVENTUAL||!!t?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(t,r){t.classList.toggle("cdk-focused",!!r),t.classList.toggle("cdk-touch-focused",r==="touch"),t.classList.toggle("cdk-keyboard-focused",r==="keyboard"),t.classList.toggle("cdk-mouse-focused",r==="mouse"),t.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(t,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=t,this._originFromTouchInteraction=t==="touch"&&r,this._detectionMode===gs.IMMEDIATE){clearTimeout(this._originTimeoutId);let i=this._originFromTouchInteraction?O_:1;this._originTimeoutId=setTimeout(()=>this._origin=null,i)}})}_onFocus(t,r){let i=this._elementInfo.get(r),o=kt(t);!i||!i.checkChildren&&r!==o||this._originChanged(r,this._getFocusOrigin(o),i)}_onBlur(t,r){let i=this._elementInfo.get(r);!i||i.checkChildren&&t.relatedTarget instanceof Node&&r.contains(t.relatedTarget)||(this._setClasses(r),this._emitOrigin(i,null))}_emitOrigin(t,r){t.subject.observers.length&&this._ngZone.run(()=>t.subject.next(r))}_registerGlobalListeners(t){if(!this._platform.isBrowser)return;let r=t.rootNode,i=this._rootNodeFocusListenerCount.get(r)||0;i||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,Il),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,Il)}),this._rootNodeFocusListenerCount.set(r,i+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(gt(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(t){let r=t.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let i=this._rootNodeFocusListenerCount.get(r);i>1?this._rootNodeFocusListenerCount.set(r,i-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Il),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Il),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(t,r,i){this._setClasses(t,r),this._emitOrigin(i,r),this._lastFocusOrigin=r}_getClosestElementsInfo(t){let r=[];return this._elementInfo.forEach((i,o)=>{(o===t||i.checkChildren&&o.contains(t))&&r.push([o,i])}),r}_isLastInteractionFromInputLabel(t){let{_mostRecentTarget:r,mostRecentModality:i}=this._inputModalityDetector;if(i!=="mouse"||!r||r===t||t.nodeName!=="INPUT"&&t.nodeName!=="TEXTAREA"||t.disabled)return!1;let o=t.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var P_=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(r,i){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return e})(),xl;function OT(){if(xl===void 0&&(xl=null,typeof window<"u")){let e=window;e.trustedTypes!==void 0&&(xl=e.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return xl}function Li(e){return OT()?.createHTML(e)||e}var L_=new Set,Or,sh=(()=>{class e{_platform=f(Ot);_nonce=f(Er,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):FT}matchMedia(t){return(this._platform.WEBKIT||this._platform.BLINK)&&kT(t,this._nonce),this._matchMedia(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function kT(e,n){if(!L_.has(e))try{Or||(Or=document.createElement("style"),n&&Or.setAttribute("nonce",n),Or.setAttribute("type","text/css"),document.head.appendChild(Or)),Or.sheet&&(Or.sheet.insertRule(`@media ${e} {body{ }}`,0),L_.add(e))}catch(t){console.error(t)}}function FT(e){return{matches:e==="all"||e==="",media:e,addListener:()=>{},removeListener:()=>{}}}var PT=(()=>{class e{create(t){return typeof MutationObserver>"u"?null:new MutationObserver(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var j_=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({providers:[PT]})}return e})();var ah={},$n=class e{_appId=f(di);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,t=!1){return this._appId!=="ng"&&(n+=this._appId),ah.hasOwnProperty(n)||(ah[n]=0),`${n}${t?e._infix+"-":""}${ah[n]++}`}static \u0275fac=function(t){return new(t||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})};var LT=new m("cdk-dir-doc",{providedIn:"root",factory:()=>f(U)}),jT=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function V_(e){let n=e?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?jT.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Ml=(()=>{class e{get value(){return this.valueSignal()}valueSignal=We("ltr");change=new re;constructor(){let t=f(LT,{optional:!0});if(t){let r=t.body?t.body.dir:null,i=t.documentElement?t.documentElement.dir:null;this.valueSignal.set(V_(r||i||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var He=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({})}return e})();var ch=class{_box;_destroyed=new H;_resizeSubject=new H;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(t=>this._resizeSubject.next(t)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new j(t=>{let r=this._resizeSubject.subscribe(t);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),r.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Se(t=>t.some(r=>r.target===n)),ad({bufferSize:1,refCount:!0}),gt(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},B_=(()=>{class e{_cleanupErrorListener;_observers=new Map;_ngZone=f($);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,t]of this._observers)t.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(t,r){let i=r?.box||"content-box";return this._observers.has(i)||this._observers.set(i,new ch(i)),this._observers.get(i).observe(t)}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var VT=new m("MATERIAL_ANIMATIONS"),H_=null;function lh(){return f(VT,{optional:!0})?.animationsDisabled||f(sf,{optional:!0})==="NoopAnimations"?"di-disabled":(H_??=f(sh).matchMedia("(prefers-reduced-motion)").matches,H_?"reduced-motion":"enabled")}function zn(){return lh()!=="enabled"}var BT=["notch"],HT=["matFormFieldNotchedOutline",""],UT=["*"],U_=["iconPrefixContainer"],$_=["textPrefixContainer"],z_=["iconSuffixContainer"],G_=["textSuffixContainer"],$T=["textField"],zT=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],GT=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function WT(e,n){e&1&&ae(0,"span",21)}function qT(e,n){if(e&1&&(_(0,"label",20),le(1,1),Ie(2,WT,1,0,"span",21),C()),e&2){let t=Le(2);ve("floating",t._shouldLabelFloat())("monitorResize",t._hasOutline())("id",t._labelId),ge("for",t._control.disableAutomaticLabeling?null:t._control.id),S(2),xe(!t.hideRequiredMarker&&t._control.required?2:-1)}}function ZT(e,n){if(e&1&&Ie(0,qT,3,5,"label",20),e&2){let t=Le();xe(t._hasFloatingLabel()?0:-1)}}function QT(e,n){e&1&&ae(0,"div",7)}function YT(e,n){}function KT(e,n){if(e&1&&Wt(0,YT,0,0,"ng-template",13),e&2){Le(2);let t=mi(1);ve("ngTemplateOutlet",t)}}function XT(e,n){if(e&1&&(_(0,"div",9),Ie(1,KT,1,1,null,13),C()),e&2){let t=Le();ve("matFormFieldNotchedOutlineOpen",t._shouldLabelFloat()),S(),xe(t._forceDisplayInfixLabel()?-1:1)}}function JT(e,n){e&1&&(_(0,"div",10,2),le(2,2),C())}function eA(e,n){e&1&&(_(0,"div",11,3),le(2,3),C())}function tA(e,n){}function nA(e,n){if(e&1&&Wt(0,tA,0,0,"ng-template",13),e&2){Le();let t=mi(1);ve("ngTemplateOutlet",t)}}function rA(e,n){e&1&&(_(0,"div",14,4),le(2,4),C())}function iA(e,n){e&1&&(_(0,"div",15,5),le(2,5),C())}function oA(e,n){e&1&&ae(0,"div",16)}function sA(e,n){e&1&&(_(0,"div",18),le(1,6),C())}function aA(e,n){if(e&1&&(_(0,"mat-hint",22),q(1),C()),e&2){let t=Le(2);ve("id",t._hintLabelId),S(),nt(t.hintLabel)}}function cA(e,n){if(e&1&&(_(0,"div",19),Ie(1,aA,2,2,"mat-hint",22),le(2,7),ae(3,"div",23),le(4,8),C()),e&2){let t=Le();S(),xe(t.hintLabel?1:-1)}}var dh=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,selectors:[["mat-label"]]})}return e})(),lA=new m("MatError");var uh=(()=>{class e{align="start";id=f($n).getId("mat-mdc-hint-");static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(r,i){r&2&&(wr("id",i.id),ge("align",null),se("mat-mdc-form-field-hint-end",i.align==="end"))},inputs:{align:"align",id:"id"}})}return e})(),dA=new m("MatPrefix");var uA=new m("MatSuffix");var X_=new m("FloatingLabelParent"),W_=(()=>{class e{_elementRef=f(J);get floating(){return this._floating}set floating(t){this._floating=t,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(t){this._monitorResize=t,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=f(B_);_ngZone=f($);_parent=f(X_);_resizeSubscription=new pe;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return fA(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(r,i){r&2&&se("mdc-floating-label--float-above",i.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return e})();function fA(e){let n=e;if(n.offsetParent!==null)return n.scrollWidth;let t=n.cloneNode(!0);t.style.setProperty("position","absolute"),t.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(t);let r=t.scrollWidth;return t.remove(),r}var q_="mdc-line-ripple--active",Sl="mdc-line-ripple--deactivating",Z_=(()=>{class e{_elementRef=f(J);_cleanupTransitionEnd;constructor(){let t=f($),r=f(St);t.runOutsideAngular(()=>{this._cleanupTransitionEnd=r.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let t=this._elementRef.nativeElement.classList;t.remove(Sl),t.add(q_)}deactivate(){this._elementRef.nativeElement.classList.add(Sl)}_handleTransitionEnd=t=>{let r=this._elementRef.nativeElement.classList,i=r.contains(Sl);t.propertyName==="opacity"&&i&&r.remove(q_,Sl)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return e})(),Q_=(()=>{class e{_elementRef=f(J);_ngZone=f($);open=!1;_notch;ngAfterViewInit(){let t=this._elementRef.nativeElement,r=t.querySelector(".mdc-floating-label");r?(t.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(r.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>r.style.transitionDuration="")}))):t.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(t){let r=this._notch.nativeElement;!this.open||!t?r.style.width="":r.style.width=`calc(${t}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(t){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${t}px)`)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(r,i){if(r&1&&dn(BT,5),r&2){let o;ee(o=te())&&(i._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(r,i){r&2&&se("mdc-notched-outline--notched",i.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:HT,ngContentSelectors:UT,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(r,i){r&1&&(tt(),Tt(0,"div",1),Pe(1,"div",2,0),le(3),qe(),Tt(4,"div",3))},encapsulation:2,changeDetection:0})}return e})(),pA=(()=>{class e{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e})}return e})();var hA=new m("MatFormField"),mA=new m("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Y_="fill",gA="auto",K_="fixed",vA="translateY(-50%)",J_=(()=>{class e{_elementRef=f(J);_changeDetectorRef=f(Rt);_platform=f(Ot);_idGenerator=f($n);_ngZone=f($);_defaults=f(mA,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ko("iconPrefixContainer");_textPrefixContainerSignal=ko("textPrefixContainer");_iconSuffixContainerSignal=ko("iconSuffixContainer");_textSuffixContainerSignal=ko("textSuffixContainer");_prefixSuffixContainers=At(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(t=>t?.nativeElement).filter(t=>t!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Sy(dh);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(t){this._hideRequiredMarker=M_(t)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||gA}set floatLabel(t){t!==this._floatLabel&&(this._floatLabel=t,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(t){let r=t||this._defaults?.appearance||Y_;this._appearanceSignal.set(r)}_appearanceSignal=We(Y_);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||K_}set subscriptSizing(t){this._subscriptSizing=t||this._defaults?.subscriptSizing||K_}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(t){this._hintLabel=t,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(t){this._explicitFormFieldControl=t}_destroyed=new H;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=zn();constructor(){let t=this._defaults,r=f(Ml);t&&(t.appearance&&(this.appearance=t.appearance),this._hideRequiredMarker=!!t?.hideRequiredMarker,t.color&&(this.color=t.color)),hu(()=>this._currentDirection=r.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=At(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(t){let r=this._control,i="mat-mdc-form-field-type-";t&&this._elementRef.nativeElement.classList.remove(i+t.controlType),r.controlType&&this._elementRef.nativeElement.classList.add(i+r.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=r.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=r.stateChanges.pipe(Yi([void 0,void 0]),L(()=>[r.errorState,r.userAriaDescribedBy]),id(),Se(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),r.ngControl&&r.ngControl.valueChanges&&(this._valueChanges=r.ngControl.valueChanges.pipe(gt(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(t=>!t._isText),this._hasTextPrefix=!!this._prefixChildren.find(t=>t._isText),this._hasIconSuffix=!!this._suffixChildren.find(t=>!t._isText),this._hasTextSuffix=!!this._suffixChildren.find(t=>t._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Zi(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let t=this._control.focused;t&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!t&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",t),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",t)}_syncOutlineLabelOffset(){Ry({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let t of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(t,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:t=>this._writeOutlinedLabelStyles(t())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=At(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(t){let r=this._control?this._control.ngControl:null;return r&&r[t]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let t=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&t.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?t.push(o.id):this._hintLabel&&t.push(this._hintLabelId),s&&t.push(s.id)}else this._errorChildren&&t.push(...this._errorChildren.map(o=>o.id));let r=this._control.describedByIds,i;if(r){let o=this._describedByIds||t;i=t.concat(r.filter(s=>s&&!o.includes(s)))}else i=t;this._control.setDescribedByIds(i),this._describedByIds=t}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let t=this._iconPrefixContainer?.nativeElement,r=this._textPrefixContainer?.nativeElement,i=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=t?.getBoundingClientRect().width??0,a=r?.getBoundingClientRect().width??0,c=i?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",u=`${s+a}px`,h=`calc(${d} * (${u} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,g=`var(--mat-mdc-form-field-label-transform, ${vA} translateX(${h}))`,I=s+a+c+l;return[g,I]}_writeOutlinedLabelStyles(t){if(t!==null){let[r,i]=t;this._floatingLabel&&(this._floatingLabel.element.style.transform=r),i!==null&&this._notchedOutline?._setMaxWidth(i)}}_isAttachedToDom(){let t=this._elementRef.nativeElement;if(t.getRootNode){let r=t.getRootNode();return r&&r!==t}return document.documentElement.contains(t)}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["mat-form-field"]],contentQueries:function(r,i,o){if(r&1&&(yc(o,i._labelChild,dh,5),hi(o,pA,5)(o,dA,5)(o,uA,5)(o,lA,5)(o,uh,5)),r&2){_c();let s;ee(s=te())&&(i._formFieldControl=s.first),ee(s=te())&&(i._prefixChildren=s),ee(s=te())&&(i._suffixChildren=s),ee(s=te())&&(i._errorChildren=s),ee(s=te())&&(i._hintChildren=s)}},viewQuery:function(r,i){if(r&1&&(bc(i._iconPrefixContainerSignal,U_,5)(i._textPrefixContainerSignal,$_,5)(i._iconSuffixContainerSignal,z_,5)(i._textSuffixContainerSignal,G_,5),dn($T,5)(U_,5)($_,5)(z_,5)(G_,5)(W_,5)(Q_,5)(Z_,5)),r&2){_c(4);let o;ee(o=te())&&(i._textField=o.first),ee(o=te())&&(i._iconPrefixContainer=o.first),ee(o=te())&&(i._textPrefixContainer=o.first),ee(o=te())&&(i._iconSuffixContainer=o.first),ee(o=te())&&(i._textSuffixContainer=o.first),ee(o=te())&&(i._floatingLabel=o.first),ee(o=te())&&(i._notchedOutline=o.first),ee(o=te())&&(i._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(r,i){r&2&&se("mat-mdc-form-field-label-always-float",i._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",i._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",i._hasIconSuffix)("mat-form-field-invalid",i._control.errorState)("mat-form-field-disabled",i._control.disabled)("mat-form-field-autofilled",i._control.autofilled)("mat-form-field-appearance-fill",i.appearance=="fill")("mat-form-field-appearance-outline",i.appearance=="outline")("mat-form-field-hide-placeholder",i._hasFloatingLabel()&&!i._shouldLabelFloat())("mat-primary",i.color!=="accent"&&i.color!=="warn")("mat-accent",i.color==="accent")("mat-warn",i.color==="warn")("ng-untouched",i._shouldForward("untouched"))("ng-touched",i._shouldForward("touched"))("ng-pristine",i._shouldForward("pristine"))("ng-dirty",i._shouldForward("dirty"))("ng-valid",i._shouldForward("valid"))("ng-invalid",i._shouldForward("invalid"))("ng-pending",i._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Qt([{provide:hA,useExisting:e},{provide:X_,useExisting:e}])],ngContentSelectors:GT,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(r,i){if(r&1&&(tt(zT),Wt(0,ZT,1,1,"ng-template",null,0,Ro),_(2,"div",6,1),Ze("click",function(s){return i._control.onContainerClick(s)}),Ie(4,QT,1,0,"div",7),_(5,"div",8),Ie(6,XT,2,2,"div",9),Ie(7,JT,3,0,"div",10),Ie(8,eA,3,0,"div",11),_(9,"div",12),Ie(10,nA,1,1,null,13),le(11),C(),Ie(12,rA,3,0,"div",14),Ie(13,iA,3,0,"div",15),C(),Ie(14,oA,1,0,"div",16),C(),_(15,"div",17),Ie(16,sA,2,0,"div",18)(17,cA,5,1,"div",19),C()),r&2){let o;S(2),se("mdc-text-field--filled",!i._hasOutline())("mdc-text-field--outlined",i._hasOutline())("mdc-text-field--no-label",!i._hasFloatingLabel())("mdc-text-field--disabled",i._control.disabled)("mdc-text-field--invalid",i._control.errorState),S(2),xe(!i._hasOutline()&&!i._control.disabled?4:-1),S(2),xe(i._hasOutline()?6:-1),S(),xe(i._hasIconPrefix?7:-1),S(),xe(i._hasTextPrefix?8:-1),S(2),xe(!i._hasOutline()||i._forceDisplayInfixLabel()?10:-1),S(2),xe(i._hasTextSuffix?12:-1),S(),xe(i._hasIconSuffix?13:-1),S(),xe(i._hasOutline()?-1:14),S(),se("mat-mdc-form-field-subscript-dynamic-size",i.subscriptSizing==="dynamic");let s=i._getSubscriptMessageType();S(),xe((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[W_,Q_,Po,Z_,uh],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return e})();var eD=(()=>{class e{isErrorState(t,r){return!!(t&&t.invalid&&(t.touched||r&&r.submitted))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var ys=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[j_,J_,He]})}return e})();var tD=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[ys,ys,A_,He]})}return e})();var pt=(function(e){return e[e.FADING_IN=0]="FADING_IN",e[e.VISIBLE=1]="VISIBLE",e[e.FADING_OUT=2]="FADING_OUT",e[e.HIDDEN=3]="HIDDEN",e})(pt||{}),fh=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=pt.HIDDEN;constructor(n,t,r,i=!1){this._renderer=n,this.element=t,this.config=r,this._animationForciblyDisabledThroughCss=i}fadeOut(){this._renderer.fadeOutRipple(this)}},nD=Pi({passive:!0,capture:!0}),ph=class{_events=new Map;addHandler(n,t,r,i){let o=this._events.get(t);if(o){let s=o.get(r);s?s.add(i):o.set(r,new Set([i]))}else this._events.set(t,new Map([[r,new Set([i])]])),n.runOutsideAngular(()=>{document.addEventListener(t,this._delegateEventHandler,nD)})}removeHandler(n,t,r){let i=this._events.get(n);if(!i)return;let o=i.get(t);o&&(o.delete(r),o.size===0&&i.delete(t),i.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,nD)))}_delegateEventHandler=n=>{let t=kt(n);t&&this._events.get(n.type)?.forEach((r,i)=>{(i===t||i.contains(t))&&r.forEach(o=>o.handleEvent(n))})}},bs={enterDuration:225,exitDuration:150},yA=800,rD=Pi({passive:!0,capture:!0}),iD=["mousedown","touchstart"],oD=["mouseup","mouseleave","touchend","touchcancel"],bA=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return e})(),Tl=class e{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new ph;constructor(n,t,r,i,o){this._target=n,this._ngZone=t,this._platform=i,i.isBrowser&&(this._containerElement=Hn(r)),o&&o.get(Un).load(bA)}fadeInRipple(n,t,r={}){let i=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=v(v({},bs),r.animation);r.centered&&(n=i.left+i.width/2,t=i.top+i.height/2);let s=r.radius||_A(n,t,i),a=n-i.left,c=t-i.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let u=window.getComputedStyle(d),p=u.transitionProperty,h=u.transitionDuration,g=p==="none"||h==="0s"||h==="0s, 0s"||i.width===0&&i.height===0,I=new fh(this,d,r,g);d.style.transform="scale3d(1, 1, 1)",I.state=pt.FADING_IN,r.persistent||(this._mostRecentTransientRipple=I);let D=null;return!g&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let w=()=>{D&&(D.fallbackTimer=null),clearTimeout(Qe),this._finishRippleTransition(I)},ce=()=>this._destroyRipple(I),Qe=setTimeout(ce,l+100);d.addEventListener("transitionend",w),d.addEventListener("transitioncancel",ce),D={onTransitionEnd:w,onTransitionCancel:ce,fallbackTimer:Qe}}),this._activeRipples.set(I,D),(g||!l)&&this._finishRippleTransition(I),I}fadeOutRipple(n){if(n.state===pt.FADING_OUT||n.state===pt.HIDDEN)return;let t=n.element,r=v(v({},bs),n.config.animation);t.style.transitionDuration=`${r.exitDuration}ms`,t.style.opacity="0",n.state=pt.FADING_OUT,(n._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let t=Hn(n);!this._platform.isBrowser||!t||t===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=t,iD.forEach(r=>{e._eventManager.addHandler(this._ngZone,r,t,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{oD.forEach(t=>{this._triggerElement.addEventListener(t,this,rD)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===pt.FADING_IN?this._startFadeOutTransition(n):n.state===pt.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let t=n===this._mostRecentTransientRipple,{persistent:r}=n.config;n.state=pt.VISIBLE,!r&&(!t||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let t=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=pt.HIDDEN,t!==null&&(n.element.removeEventListener("transitionend",t.onTransitionEnd),n.element.removeEventListener("transitioncancel",t.onTransitionCancel),t.fallbackTimer!==null&&clearTimeout(t.fallbackTimer)),n.element.remove()}_onMousedown(n){let t=hs(n),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+yA;!this._target.rippleDisabled&&!t&&!r&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!ms(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let t=n.changedTouches;if(t)for(let r=0;r<t.length;r++)this.fadeInRipple(t[r].clientX,t[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let t=n.state===pt.VISIBLE||n.config.terminateOnPointerUp&&n.state===pt.FADING_IN;!n.config.persistent&&t&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(iD.forEach(t=>e._eventManager.removeHandler(t,n,this)),this._pointerUpEventsRegistered&&(oD.forEach(t=>n.removeEventListener(t,this,rD)),this._pointerUpEventsRegistered=!1))}};function _A(e,n,t){let r=Math.max(Math.abs(e-t.left),Math.abs(e-t.right)),i=Math.max(Math.abs(n-t.top),Math.abs(n-t.bottom));return Math.sqrt(r*r+i*i)}var Al=new m("mat-ripple-global-options");var DA={capture:!0},CA=["focus","mousedown","mouseenter","touchstart"],hh="mat-ripple-loader-uninitialized",mh="mat-ripple-loader-class-name",sD="mat-ripple-loader-centered",Rl="mat-ripple-loader-disabled",Nl=(()=>{class e{_document=f(U);_animationsDisabled=zn();_globalRippleOptions=f(Al,{optional:!0});_platform=f(Ot);_ngZone=f($);_injector=f(he);_eventCleanups;_hosts=new Map;constructor(){let t=f(xt).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>CA.map(r=>t.listen(this._document,r,this._onInteraction,DA)))}ngOnDestroy(){let t=this._hosts.keys();for(let r of t)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(t,r){t.setAttribute(hh,this._globalRippleOptions?.namespace??""),(r.className||!t.hasAttribute(mh))&&t.setAttribute(mh,r.className||""),r.centered&&t.setAttribute(sD,""),r.disabled&&t.setAttribute(Rl,"")}setDisabled(t,r){let i=this._hosts.get(t);i?(i.target.rippleDisabled=r,!r&&!i.hasSetUpEvents&&(i.hasSetUpEvents=!0,i.renderer.setupTriggerEvents(t))):r?t.setAttribute(Rl,""):t.removeAttribute(Rl)}_onInteraction=t=>{let r=kt(t);if(r instanceof HTMLElement){let i=r.closest(`[${hh}="${this._globalRippleOptions?.namespace??""}"]`);i&&this._createRipple(i)}};_createRipple(t){if(!this._document||this._hosts.has(t))return;t.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",t.getAttribute(mh)),t.append(r);let i=this._globalRippleOptions,o=this._animationsDisabled?0:i?.animation?.enterDuration??bs.enterDuration,s=this._animationsDisabled?0:i?.animation?.exitDuration??bs.exitDuration,a={rippleDisabled:this._animationsDisabled||i?.disabled||t.hasAttribute(Rl),rippleConfig:{centered:t.hasAttribute(sD),terminateOnPointerUp:i?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new Tl(a,this._ngZone,r,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(t),this._hosts.set(t,{target:a,renderer:c,hasSetUpEvents:l}),t.removeAttribute(hh)}destroyRipple(t){let r=this._hosts.get(t);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(t))}static \u0275fac=function(r){return new(r||e)};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();var _s=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,i){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var EA=new m("MAT_BUTTON_CONFIG");function aD(e){return e==null?void 0:Fn(e)}var cD=(()=>{class e{_elementRef=f(J);_ngZone=f($);_animationsDisabled=zn();_config=f(EA,{optional:!0});_focusMonitor=f(vs);_cleanupClick;_renderer=f(St);_rippleLoader=f(Nl);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(t){this._disableRipple=t,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(t){this._disabled=t,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(t){this.tabIndex=t}constructor(){f(Un).load(_s);let t=this._elementRef.nativeElement;this._isAnchor=t.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(t,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(t="program",r){t?this._focusMonitor.focusVia(this._elementRef.nativeElement,t,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",t=>{this.disabled&&(t.preventDefault(),t.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(r,i){r&2&&(ge("disabled",i._getDisabledAttribute())("aria-disabled",i._getAriaDisabled())("tabindex",i._getTabIndex()),un(i.color?"mat-"+i.color:""),se("mat-mdc-button-disabled",i.disabled)("mat-mdc-button-disabled-interactive",i.disabledInteractive)("mat-unthemed",!i.color)("_mat-animation-noopable",i._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",Ve],disabled:[2,"disabled","disabled",Ve],ariaDisabled:[2,"aria-disabled","ariaDisabled",Ve],disabledInteractive:[2,"disabledInteractive","disabledInteractive",Ve],tabIndex:[2,"tabIndex","tabIndex",aD],_tabindex:[2,"tabindex","_tabindex",aD]}})}return e})();var Ol=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[He]})}return e})();var wA=["matButton",""],IA=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],xA=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var lD=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),dD=(()=>{class e extends cD{get appearance(){return this._appearance}set appearance(t){this.setAppearance(t||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let t=MA(this._elementRef.nativeElement);t&&this.setAppearance(t)}setAppearance(t){if(t===this._appearance)return;let r=this._elementRef.nativeElement.classList,i=this._appearance?lD.get(this._appearance):null,o=lD.get(t);i&&r.remove(...i),r.add(...o),this._appearance=t}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[lt],attrs:wA,ngContentSelectors:xA,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(r,i){r&1&&(tt(IA),Tt(0,"span",0),le(1),Pe(2,"span",1),le(3,1),qe(),le(4,2),Tt(5,"span",2)(6,"span",3)),r&2&&se("mdc-button__ripple",!i._isFab)("mdc-fab__ripple",i._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return e})();function MA(e){return e.hasAttribute("mat-raised-button")?"elevated":e.hasAttribute("mat-stroked-button")?"outlined":e.hasAttribute("mat-flat-button")?"filled":e.hasAttribute("mat-button")?"text":null}var uD=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[Ol,He]})}return e})();var TA=["*"];var AA=new m("MAT_CARD_CONFIG"),fD=(()=>{class e{appearance;constructor(){let t=f(AA,{optional:!0});this.appearance=t?.appearance||"raised"}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(r,i){r&2&&se("mat-mdc-card-outlined",i.appearance==="outlined")("mdc-card--outlined",i.appearance==="outlined")("mat-mdc-card-filled",i.appearance==="filled")("mdc-card--filled",i.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:TA,decls:1,vars:0,template:function(r,i){r&1&&(tt(),le(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return e})();var pD=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[He]})}return e})();var NA=["determinateSpinner"];function OA(e,n){if(e&1&&(Rn(),_(0,"svg",11),ae(1,"circle",12),C()),e&2){let t=Le();ge("viewBox",t._viewBox()),S(),gi("stroke-dasharray",t._strokeCircumference(),"px")("stroke-dashoffset",t._strokeCircumference()/2,"px")("stroke-width",t._circleStrokeWidth(),"%"),ge("r",t._circleRadius())}}var kA=new m("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:hD})}),hD=100,FA=10,mD=(()=>{class e{_elementRef=f(J);_noopAnimations;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;_defaultColor="primary";_determinateCircle;constructor(){let t=f(kA),r=lh(),i=this._elementRef.nativeElement;this._noopAnimations=r==="di-disabled"&&!!t&&!t._forceAnimations,this.mode=i.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&r==="reduced-motion"&&i.classList.add("mat-progress-spinner-reduced-motion"),t&&(t.color&&(this.color=this._defaultColor=t.color),t.diameter&&(this.diameter=t.diameter),t.strokeWidth&&(this.strokeWidth=t.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(t){this._value=Math.max(0,Math.min(100,t||0))}_value=0;get diameter(){return this._diameter}set diameter(t){this._diameter=t||0}_diameter=hD;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(t){this._strokeWidth=t||0}_strokeWidth;_circleRadius(){return(this.diameter-FA)/2}_viewBox(){let t=this._circleRadius()*2+this.strokeWidth;return`0 0 ${t} ${t}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(r,i){if(r&1&&dn(NA,5),r&2){let o;ee(o=te())&&(i._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(r,i){r&2&&(ge("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",i.mode==="determinate"?i.value:null)("mode",i.mode),un("mat-"+i.color),gi("width",i.diameter,"px")("height",i.diameter,"px")("--mat-progress-spinner-size",i.diameter+"px")("--mat-progress-spinner-active-indicator-width",i.diameter+"px"),se("_mat-animation-noopable",i._noopAnimations)("mdc-circular-progress--indeterminate",i.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",Fn],diameter:[2,"diameter","diameter",Fn],strokeWidth:[2,"strokeWidth","strokeWidth",Fn]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(r,i){if(r&1&&(Wt(0,OA,2,8,"ng-template",null,0,Ro),_(2,"div",2,1),Rn(),_(4,"svg",3),ae(5,"circle",4),C()(),ni(),_(6,"div",5)(7,"div",6)(8,"div",7),pi(9,8),C(),_(10,"div",9),pi(11,8),C(),_(12,"div",10),pi(13,8),C()()()),r&2){let o=mi(1);S(4),ge("viewBox",i._viewBox()),S(),gi("stroke-dasharray",i._strokeCircumference(),"px")("stroke-dashoffset",i._strokeDashOffset(),"px")("stroke-width",i._circleStrokeWidth(),"%"),ge("r",i._circleRadius()),S(4),ve("ngTemplateOutlet",o),S(2),ve("ngTemplateOutlet",o),S(2),ve("ngTemplateOutlet",o)}},dependencies:[Po],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return e})();var gD=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[He]})}return e})();var LA=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],jA=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function VA(e,n){e&1&&(_(0,"span",3),le(1,1),C())}function BA(e,n){e&1&&(_(0,"span",6),le(1,2),C())}var HA=new m("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),vD=new m("MatChipAvatar"),yD=new m("MatChipTrailingIcon"),bD=new m("MatChipEdit"),_D=new m("MatChipRemove"),CD=new m("MatChip"),ED=(()=>{class e{_elementRef=f(J);_parentChip=f(CD);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(t){this._disabled=t}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){f(Un).load(_s),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(r){return new(r||e)};static \u0275dir=Q({type:e,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(r,i){r&2&&(ge("disabled",i._getDisabledAttribute())("aria-disabled",i.disabled),se("mdc-evolution-chip__action--primary",i._isPrimary)("mdc-evolution-chip__action--secondary",!i._isPrimary)("mdc-evolution-chip__action--trailing",!i._isPrimary&&!i._isLeading))},inputs:{disabled:[2,"disabled","disabled",Ve],tabIndex:[2,"tabIndex","tabIndex",t=>t==null?-1:Fn(t)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return e})(),UA=(()=>{class e extends ED{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(t){!this.disabled&&this._isPrimary&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(t){(t.keyCode===13||t.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(t.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let t;return function(i){return(t||(t=an(e)))(i||e)}})();static \u0275dir=Q({type:e,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(r,i){r&1&&Ze("click",function(s){return i._handleClick(s)})("keydown",function(s){return i._handleKeydown(s)}),r&2&&(ge("tabindex",i._getTabindex()),se("mdc-evolution-chip__action--presentational",!1))},features:[lt]})}return e})();var wD=(()=>{class e{_changeDetectorRef=f(Rt);_elementRef=f(J);_tagName=f(xy);_ngZone=f($);_focusMonitor=f(vs);_globalRippleOptions=f(Al,{optional:!0});_document=f(U);_onFocus=new H;_onBlur=new H;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=zn();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=f($n).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(t){this._value=t}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(t){this._disabled=t}_disabled=!1;removed=new re;destroyed=new re;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=f(Nl);_injector=f(he);constructor(){let t=f(Un);t.load(_s),t.load(P_),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Zi(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe(),this.destroyed.emit({chip:this}),this.destroyed.complete()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(t){(t.keyCode===8&&!t.repeat||t.keyCode===46)&&(t.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(t){return this._getActions().find(r=>{let i=r._elementRef.nativeElement;return i===t||i.contains(t)})}_getActions(){let t=[];return this.editIcon&&t.push(this.editIcon),this.primaryAction&&t.push(this.primaryAction),this.removeIcon&&t.push(this.removeIcon),t}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(t){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(t=>{let r=t!==null;r!==this._hasFocusInternal&&(this._hasFocusInternal=r,r?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(r,i,o){if(r&1&&hi(o,vD,5)(o,bD,5)(o,yD,5)(o,_D,5)(o,vD,5)(o,yD,5)(o,bD,5)(o,_D,5),r&2){let s;ee(s=te())&&(i.leadingIcon=s.first),ee(s=te())&&(i.editIcon=s.first),ee(s=te())&&(i.trailingIcon=s.first),ee(s=te())&&(i.removeIcon=s.first),ee(s=te())&&(i._allLeadingIcons=s),ee(s=te())&&(i._allTrailingIcons=s),ee(s=te())&&(i._allEditIcons=s),ee(s=te())&&(i._allRemoveIcons=s)}},viewQuery:function(r,i){if(r&1&&dn(UA,5),r&2){let o;ee(o=te())&&(i.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(r,i){r&1&&Ze("keydown",function(s){return i._handleKeydown(s)}),r&2&&(wr("id",i.id),ge("role",i.role)("aria-label",i.ariaLabel),un("mat-"+(i.color||"primary")),se("mdc-evolution-chip",!i._isBasicChip)("mdc-evolution-chip--disabled",i.disabled)("mdc-evolution-chip--with-trailing-action",i._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",i.leadingIcon)("mdc-evolution-chip--with-primary-icon",i.leadingIcon)("mdc-evolution-chip--with-avatar",i.leadingIcon)("mat-mdc-chip-with-avatar",i.leadingIcon)("mat-mdc-chip-highlighted",i.highlighted)("mat-mdc-chip-disabled",i.disabled)("mat-mdc-basic-chip",i._isBasicChip)("mat-mdc-standard-chip",!i._isBasicChip)("mat-mdc-chip-with-trailing-icon",i._hasTrailingIcon())("_mat-animation-noopable",i._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",Ve],highlighted:[2,"highlighted","highlighted",Ve],disableRipple:[2,"disableRipple","disableRipple",Ve],disabled:[2,"disabled","disabled",Ve]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[Qt([{provide:CD,useExisting:e}])],ngContentSelectors:jA,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(r,i){r&1&&(tt(LA),ae(0,"span",0),_(1,"span",1)(2,"span",2),Ie(3,VA,2,0,"span",3),_(4,"span",4),le(5),ae(6,"span",5),C()()(),Ie(7,BA,2,0,"span",6)),r&2&&(S(3),xe(i.leadingIcon?3:-1),S(4),xe(i._hasTrailingIcon()?7:-1))},dependencies:[ED],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2,changeDetection:0})}return e})();var ID=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({providers:[eD,{provide:HA,useValue:{separatorKeyCodes:[13]}}],imports:[Ol,He]})}return e})();function xD(e){return Error(`Unable to find icon with the name "${e}"`)}function zA(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function MD(e){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${e}".`)}function SD(e){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${e}".`)}var gn=class{url;svgText;options;svgElement=null;constructor(n,t,r){this.url=n,this.svgText=t,this.options=r}},AD=(()=>{class e{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(t,r,i,o){this._httpClient=t,this._sanitizer=r,this._errorHandler=o,this._document=i}addSvgIcon(t,r,i){return this.addSvgIconInNamespace("",t,r,i)}addSvgIconLiteral(t,r,i){return this.addSvgIconLiteralInNamespace("",t,r,i)}addSvgIconInNamespace(t,r,i,o){return this._addSvgIconConfig(t,r,new gn(i,null,o))}addSvgIconResolver(t){return this._resolvers.push(t),this}addSvgIconLiteralInNamespace(t,r,i,o){let s=this._sanitizer.sanitize(Fe.HTML,i);if(!s)throw SD(i);let a=Li(s);return this._addSvgIconConfig(t,r,new gn("",a,o))}addSvgIconSet(t,r){return this.addSvgIconSetInNamespace("",t,r)}addSvgIconSetLiteral(t,r){return this.addSvgIconSetLiteralInNamespace("",t,r)}addSvgIconSetInNamespace(t,r,i){return this._addSvgIconSetConfig(t,new gn(r,null,i))}addSvgIconSetLiteralInNamespace(t,r,i){let o=this._sanitizer.sanitize(Fe.HTML,r);if(!o)throw SD(r);let s=Li(o);return this._addSvgIconSetConfig(t,new gn("",s,i))}registerFontClassAlias(t,r=t){return this._fontCssClassesByAlias.set(t,r),this}classNameForFontAlias(t){return this._fontCssClassesByAlias.get(t)||t}setDefaultFontSetClass(...t){return this._defaultFontSetClass=t,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(t){let r=this._sanitizer.sanitize(Fe.RESOURCE_URL,t);if(!r)throw MD(t);let i=this._cachedIconsByUrl.get(r);return i?M(kl(i)):this._loadSvgIconFromConfig(new gn(t,null)).pipe(Ne(o=>this._cachedIconsByUrl.set(r,o)),L(o=>kl(o)))}getNamedSvgIcon(t,r=""){let i=TD(r,t),o=this._svgIconConfigs.get(i);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(r,t),o)return this._svgIconConfigs.set(i,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(r);return s?this._getSvgFromIconSetConfigs(t,s):Jn(xD(i))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(t){return t.svgText?M(kl(this._svgElementFromConfig(t))):this._loadSvgIconFromConfig(t).pipe(L(r=>kl(r)))}_getSvgFromIconSetConfigs(t,r){let i=this._extractIconWithNameFromAnySet(t,r);if(i)return M(i);let o=r.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Pt(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(Fe.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),M(null)})));return qi(o).pipe(L(()=>{let s=this._extractIconWithNameFromAnySet(t,r);if(!s)throw xD(t);return s}))}_extractIconWithNameFromAnySet(t,r){for(let i=r.length-1;i>=0;i--){let o=r[i];if(o.svgText&&o.svgText.toString().indexOf(t)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,t,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(t){return this._fetchIcon(t).pipe(Ne(r=>t.svgText=r),L(()=>this._svgElementFromConfig(t)))}_loadSvgIconSetFromConfig(t){return t.svgText?M(null):this._fetchIcon(t).pipe(Ne(r=>t.svgText=r))}_extractSvgIconFromSet(t,r,i){let o=t.querySelector(`[id="${r}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,i);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),i);let a=this._svgElementFromString(Li("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,i)}_svgElementFromString(t){let r=this._document.createElement("DIV");r.innerHTML=t;let i=r.querySelector("svg");if(!i)throw Error("<svg> tag not found");return i}_toSvgElement(t){let r=this._svgElementFromString(Li("<svg></svg>")),i=t.attributes;for(let o=0;o<i.length;o++){let{name:s,value:a}=i[o];s!=="id"&&r.setAttribute(s,a)}for(let o=0;o<t.childNodes.length;o++)t.childNodes[o].nodeType===this._document.ELEMENT_NODE&&r.appendChild(t.childNodes[o].cloneNode(!0));return r}_setSvgAttributes(t,r){return t.setAttribute("fit",""),t.setAttribute("height","100%"),t.setAttribute("width","100%"),t.setAttribute("preserveAspectRatio","xMidYMid meet"),t.setAttribute("focusable","false"),r&&r.viewBox&&t.setAttribute("viewBox",r.viewBox),t}_fetchIcon(t){let{url:r,options:i}=t,o=i?.withCredentials??!1;if(!this._httpClient)throw zA();if(r==null)throw Error(`Cannot fetch icon from URL "${r}".`);let s=this._sanitizer.sanitize(Fe.RESOURCE_URL,r);if(!s)throw MD(r);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(L(l=>Li(l)),tr(()=>this._inProgressUrlFetches.delete(s)),Qi());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(t,r,i){return this._svgIconConfigs.set(TD(t,r),i),this}_addSvgIconSetConfig(t,r){let i=this._iconSetConfigs.get(t);return i?i.push(r):this._iconSetConfigs.set(t,[r]),this}_svgElementFromConfig(t){if(!t.svgElement){let r=this._svgElementFromString(t.svgText);this._setSvgAttributes(r,t.options),t.svgElement=r}return t.svgElement}_getIconConfigFromResolvers(t,r){for(let i=0;i<this._resolvers.length;i++){let o=this._resolvers[i](r,t);if(o)return GA(o)?new gn(o.url,null,o.options):new gn(o,null)}}static \u0275fac=function(r){return new(r||e)(E(Ci,8),E(Dp),E(U,8),E(Ke))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})}return e})();function kl(e){return e.cloneNode(!0)}function TD(e,n){return e+":"+n}function GA(e){return!!(e.url&&e.options)}var WA=["*"],qA=new m("MAT_ICON_DEFAULT_OPTIONS"),ZA=new m("mat-icon-location",{providedIn:"root",factory:()=>{let e=f(U),n=e?e.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),RD=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],QA=RD.map(e=>`[${e}]`).join(", "),YA=/^url\(['"]?#(.*?)['"]?\)$/,ND=(()=>{class e{_elementRef=f(J);_iconRegistry=f(AD);_location=f(ZA);_errorHandler=f(Ke);_defaultColor;get color(){return this._color||this._defaultColor}set color(t){this._color=t}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(t){t!==this._svgIcon&&(t?this._updateSvgIcon(t):this._svgIcon&&this._clearSvgElement(),this._svgIcon=t)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(t){let r=this._cleanupFontValue(t);r!==this._fontSet&&(this._fontSet=r,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(t){let r=this._cleanupFontValue(t);r!==this._fontIcon&&(this._fontIcon=r,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=pe.EMPTY;constructor(){let t=f(new Oo("aria-hidden"),{optional:!0}),r=f(qA,{optional:!0});r&&(r.color&&(this.color=this._defaultColor=r.color),r.fontSet&&(this.fontSet=r.fontSet)),t||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(t){if(!t)return["",""];let r=t.split(":");switch(r.length){case 1:return["",r[0]];case 2:return r;default:throw Error(`Invalid icon name: "${t}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let t=this._elementsWithExternalReferences;if(t&&t.size){let r=this._location.getPathname();r!==this._previousPath&&(this._previousPath=r,this._prependPathToReferences(r))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(t){this._clearSvgElement();let r=this._location.getPathname();this._previousPath=r,this._cacheChildrenWithExternalReferences(t),this._prependPathToReferences(r),this._elementRef.nativeElement.appendChild(t)}_clearSvgElement(){let t=this._elementRef.nativeElement,r=t.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();r--;){let i=t.childNodes[r];(i.nodeType!==1||i.nodeName.toLowerCase()==="svg")&&i.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let t=this._elementRef.nativeElement,r=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(i=>i.length>0);this._previousFontSetClass.forEach(i=>t.classList.remove(i)),r.forEach(i=>t.classList.add(i)),this._previousFontSetClass=r,this.fontIcon!==this._previousFontIconClass&&!r.includes("mat-ligature-font")&&(this._previousFontIconClass&&t.classList.remove(this._previousFontIconClass),this.fontIcon&&t.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(t){return typeof t=="string"?t.trim().split(" ")[0]:t}_prependPathToReferences(t){let r=this._elementsWithExternalReferences;r&&r.forEach((i,o)=>{i.forEach(s=>{o.setAttribute(s.name,`url('${t}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(t){let r=t.querySelectorAll(QA),i=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<r.length;o++)RD.forEach(s=>{let a=r[o],c=a.getAttribute(s),l=c?c.match(YA):null;if(l){let d=i.get(a);d||(d=[],i.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(t){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),t){let[r,i]=this._splitIconName(t);r&&(this._svgNamespace=r),i&&(this._svgName=i),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(i,r).pipe(mt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${r}:${i}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(r){return new(r||e)};static \u0275cmp=W({type:e,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(r,i){r&2&&(ge("data-mat-icon-type",i._usingFontIcon()?"font":"svg")("data-mat-icon-name",i._svgName||i.fontIcon)("data-mat-icon-namespace",i._svgNamespace||i.fontSet)("fontIcon",i._usingFontIcon()?i.fontIcon:null),un(i.color?"mat-"+i.color:""),se("mat-icon-inline",i.inline)("mat-icon-no-color",i.color!=="primary"&&i.color!=="accent"&&i.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",Ve],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:WA,decls:1,vars:0,template:function(r,i){r&1&&(tt(),le(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return e})(),OD=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275mod=Z({type:e});static \u0275inj=G({imports:[He]})}return e})();var gh={production:!1,weatherApiKey:"947b61f3efe5ef3b0b06227c7ff14cec",weatherApiUrl:"https://api.openweathermap.org/data/2.5"};var Fl=class e{constructor(n){this.http=n}HISTORY_KEY="weather_history";MAX_HISTORY=10;apiUrl=gh.weatherApiUrl;apiKey=gh.weatherApiKey;cache=new Map;CACHE_TTL=600*1e3;getWeather(n){let t=n.toLowerCase(),r=this.cache.get(t),i=Date.now();if(r&&i-r.timestamp<this.CACHE_TTL)return M(r.data);let o=`${this.apiUrl}/weather?q=${n}&appid=${this.apiKey}&units=metric`;return this.http.get(o).pipe(od(1),L(s=>{let a=this.mapToDisplay(s);return this.cache.set(t,{data:a,timestamp:i}),a}),Pt(this.handleError))}handleError(n){let t;return n.status===0?t="No internet connection. Please check your network.":n.status===401?t="Invalid API key. Please check your configuration.":n.status===404?t="City not found. Please check the spelling and try again.":n.status===429?t="Too many requests. Please wait a moment and try again.":t=`Server error (code ${n.status}). Please try again later.`,Jn(()=>new Error(t))}mapToDisplay(n){return{city:n.name,country:n.sys.country,temperature:Math.round(n.main.temp),feelsLike:Math.round(n.main.feels_like),description:n.weather[0].description,humidity:n.main.humidity,windSpeed:Math.round(n.wind.speed*3.6),icon:n.weather[0].icon,tempMin:Math.round(n.main.temp_min),tempMax:Math.round(n.main.temp_max)}}getSearchHistory(){let n=localStorage.getItem(this.HISTORY_KEY);return n?JSON.parse(n):[]}addToHistory(n){let t=this.getSearchHistory();t=t.filter(r=>r.toLowerCase()!==n.toLowerCase()),t.unshift(n),t=t.slice(0,this.MAX_HISTORY),localStorage.setItem(this.HISTORY_KEY,JSON.stringify(t))}clearHistory(){localStorage.removeItem(this.HISTORY_KEY)}static \u0275fac=function(t){return new(t||e)(E(Ci))};static \u0275prov=y({token:e,factory:e.\u0275fac,providedIn:"root"})};var JA=(e,n,t,r)=>({"wind-calm":e,"wind-moderate":n,"wind-strong":t,"wind-extreme":r});function eR(e,n){e&1&&(_(0,"div",9),ae(1,"mat-progress-spinner",10),C())}function tR(e,n){e&1&&ae(0,"app-loading-spinner",11)}function nR(e,n){if(e&1){let t=Qf();_(0,"app-error-message",12),Ze("retry",function(){Ia(t);let i=Le();return xa(i.onRetry())}),C()}if(e&2){let t=Le();ve("message",t.errorMessage)}}function rR(e,n){if(e&1&&(_(0,"mat-card",13)(1,"div",14),Rn(),_(2,"svg",15)(3,"defs")(4,"linearGradient",16),ae(5,"stop",17)(6,"stop",18)(7,"stop",19)(8,"stop",20),C(),_(9,"mask",21),ae(10,"rect",22),C()(),_(11,"g",23),ae(12,"path",24)(13,"path",25)(14,"path",26)(15,"path",27)(16,"path",28)(17,"path",29)(18,"path",30)(19,"path",31)(20,"path",32)(21,"path",33)(22,"path",34)(23,"path",35)(24,"path",36),C()()(),ni(),_(25,"div",37)(26,"div",38)(27,"h2",39),q(28),C(),_(29,"span",40),q(30),C()(),ae(31,"img",41),C(),_(32,"p",42),q(33),Ec(34,"titlecase"),C(),_(35,"div",43)(36,"span",44),q(37),C(),_(38,"span",45),q(39),C()(),_(40,"div",46)(41,"span"),q(42),C(),_(43,"span"),q(44),C()(),ae(45,"div",47),_(46,"div",48)(47,"div",49)(48,"span",50),q(49,"\u{1F4A7}"),C(),_(50,"span",51),q(51,"Humidity"),C(),_(52,"span",52),q(53),C()(),_(54,"div",49)(55,"span",50),q(56,"\u{1F4A8}"),C(),_(57,"span",51),q(58,"Wind"),C(),_(59,"mat-chip",53),q(60),C()(),_(61,"div",49)(62,"span",50),q(63,"\u{1F32C}\uFE0F"),C(),_(64,"span",51),q(65,"Wind Speed"),C(),_(66,"mat-chip",53),q(67),Ec(68,"windSpeed"),C()()()()),e&2){let t=Le();ve("ngClass",Kf(17,JA,t.weatherData.windSpeed<20,t.weatherData.windSpeed>=20&&t.weatherData.windSpeed<40,t.weatherData.windSpeed>=40&&t.weatherData.windSpeed<70,t.weatherData.windSpeed>=70)),S(28),nt(t.weatherData.city),S(2),nt(t.weatherData.country),S(),ve("src","https://openweathermap.org/img/wn/"+t.weatherData.icon+"@2x.png",hf)("alt",t.weatherData.description),S(2),nt(wc(34,13,t.weatherData.description)),S(4),Zt("",t.weatherData.temperature,"\xB0C"),S(2),Zt("Feels like ",t.weatherData.feelsLike,"\xB0C"),S(3),Zt("\u2191 ",t.weatherData.tempMax,"\xB0C"),S(2),Zt("\u2193 ",t.weatherData.tempMin,"\xB0C"),S(9),Zt("",t.weatherData.humidity,"%"),S(7),Zt("",t.weatherData.windSpeed," km/h"),S(7),nt(wc(68,15,t.weatherData.windSpeed))}}var Pl=class e{constructor(n,t){this.weatherService=n;this.cdr=t}cityName="";lastCity="";hasSearched=!1;isLoading=!1;errorMessage="";weatherData=null;onSearch(){let n=this.cityName.trim();n&&(this.isLoading=!0,this.errorMessage="",this.hasSearched=!0,this.cityName="",this.lastCity=n,this.weatherService.getWeather(n).subscribe({next:t=>{this.weatherData=t,this.isLoading=!1,this.cdr.detectChanges()},error:t=>{this.errorMessage=t.status===404?"City not found. Please check the spelling.":"Something went wrong. Please try again.",this.isLoading=!1,this.weatherData=null,this.cdr.detectChanges()}}))}onRetry(){this.errorMessage="",this.onSearch()}onKeyPress(n){n.key==="Enter"&&this.onSearch()}static \u0275fac=function(t){return new(t||e)(oe(Fl),oe(Rt))};static \u0275cmp=W({type:e,selectors:[["app-weather-search"]],decls:13,vars:5,consts:[[1,"search-container"],[1,"search-title"],[1,"search-row"],["type","text","placeholder","Enter city name...",1,"search-input",3,"ngModelChange","keypress","ngModel"],["mat-raised-button","","color","primary",1,"search-btn",3,"click"],["class","spinner-container",4,"ngIf"],["message","Fetching weather data...",4,"ngIf"],[3,"message","retry",4,"ngIf"],["class","weather-card",3,"ngClass",4,"ngIf"],[1,"spinner-container"],["mode","indeterminate","diameter","40"],["message","Fetching weather data..."],[3,"retry","message"],[1,"weather-card",3,"ngClass"],[1,"wind-svg-layer"],["viewBox","0 0 660 340","preserveAspectRatio","xMidYMid slice","xmlns","http://www.w3.org/2000/svg"],["id","fadeMask","x1","0%","y1","0%","x2","100%","y2","0%"],["offset","0%","stop-color","white","stop-opacity","0"],["offset","10%","stop-color","white","stop-opacity","1"],["offset","85%","stop-color","white","stop-opacity","1"],["offset","100%","stop-color","white","stop-opacity","0"],["id","edgeMask"],["width","660","height","340","fill","url(#fadeMask)"],["mask","url(#edgeMask)"],["d","M -80 55   C 60 30,  200 80,  360 50  S 560 20,  760 60","stroke-dasharray","520 180",1,"wind-streak",2,"--dur","7s","--delay","0s"],["d","M -60 110  C 80 90,  220 140, 380 110 S 580 70,  780 115","stroke-dasharray","480 220",1,"wind-streak",2,"--dur","5.5s","--delay","0.8s"],["d","M -100 165 C 100 140,280 200, 440 162 S 640 120, 820 170","stroke-dasharray","560 160",1,"wind-streak",2,"--dur","6.2s","--delay","1.6s"],["d","M -50 215  C 90 190, 240 255, 420 215 S 620 165, 800 220","stroke-dasharray","500 200",1,"wind-streak",2,"--dur","8s","--delay","0.4s"],["d","M -90 275  C 70 250, 220 305, 410 268 S 620 220, 820 278","stroke-dasharray","540 160",1,"wind-streak",2,"--dur","6.8s","--delay","2.1s"],["d","M -30 32   C 120 10, 280 55,  460 28  S 680 -5,  850 35","stroke-dasharray","400 260",1,"wind-streak",2,"--dur","9s","--delay","3s"],["d","M -70 305  C 100 285,300 330, 500 298 S 700 255, 870 308","stroke-dasharray","460 200",1,"wind-streak",2,"--dur","7.5s","--delay","1.2s"],["d","M 820 80   C 660 55, 480 105, 300 78  S 80 35,  -80 82","stroke-dasharray","440 220",1,"wind-streak","secondary",2,"--dur","4.5s","--delay","0.6s"],["d","M 840 195  C 680 170,490 218, 310 192 S 90 145, -80 198","stroke-dasharray","480 180",1,"wind-streak","secondary",2,"--dur","5s","--delay","1.4s"],["d","M 830 255  C 660 230,460 280, 270 248 S 60 200, -70 258","stroke-dasharray","420 240",1,"wind-streak","secondary",2,"--dur","3.8s","--delay","2.3s"],["d","M -40 142  C 140 118,320 162, 510 136 S 720 96,  910 144","stroke-dasharray","360 280",1,"wind-streak","tertiary",2,"--dur","1.8s","--delay","0.15s"],["d","M -60 238  C 130 212,330 258, 520 232 S 730 190, 930 240","stroke-dasharray","380 260",1,"wind-streak","tertiary",2,"--dur","2s","--delay","0.55s"],["d","M 900 130  C 720 108,520 152, 320 128 S 90 84,  -80 132","stroke-dasharray","340 300",1,"wind-streak","tertiary",2,"--dur","1.5s","--delay","0.3s"],[1,"card-top"],[1,"city-info"],[1,"city-name"],[1,"country"],["onerror","this.style.display='none'",1,"weather-icon",3,"src","alt"],[1,"description"],[1,"temperature"],[1,"temp-main"],[1,"feels-like"],[1,"temp-range"],[1,"card-divider"],[1,"weather-details"],[1,"detail-item"],[1,"detail-icon"],[1,"detail-label"],[1,"detail-value"],[1,"detail-chip"]],template:function(t,r){t&1&&(_(0,"div",0)(1,"h2",1),q(2,"Search for a city"),C(),_(3,"div",2)(4,"input",3),Cc("ngModelChange",function(o){return Yf(r.cityName,o)||(r.cityName=o),o}),Ze("keypress",function(o){return r.onKeyPress(o)}),C(),_(5,"button",4),Ze("click",function(){return r.onSearch()}),_(6,"mat-icon"),q(7,"cloud"),C(),q(8," \u{1F324} Search "),C()(),Wt(9,eR,2,0,"div",5)(10,tR,1,0,"app-loading-spinner",6)(11,nR,1,1,"app-error-message",7)(12,rR,69,22,"mat-card",8),C()),t&2&&(S(4),Dc("ngModel",r.cityName),S(5),ve("ngIf",r.isLoading),S(),ve("ngIf",r.isLoading),S(),ve("ngIf",r.errorMessage),S(),ve("ngIf",r.weatherData&&!r.isLoading))},dependencies:[x_,bl,E_,th,Rc,ip,op,Cl,Dl,tD,uD,dD,pD,fD,gD,mD,ID,wD,OD,ND,ys,sp,El],styles:[".search-container[_ngcontent-%COMP%]{padding:.5rem 0}.search-container[_ngcontent-%COMP%]   .search-title[_ngcontent-%COMP%]{font-size:1.2rem;font-weight:600;color:#374151;margin-bottom:1.25rem}.search-container[_ngcontent-%COMP%]   .search-row[_ngcontent-%COMP%]{display:flex;gap:.75rem;margin-bottom:1.5rem}.search-container[_ngcontent-%COMP%]   .search-row[_ngcontent-%COMP%]   .search-input[_ngcontent-%COMP%]{flex:1;padding:.9rem 1.1rem;font-size:1rem;border:2px solid #e5e7eb;border-radius:.75rem;outline:none;transition:border-color .2s}.search-container[_ngcontent-%COMP%]   .search-row[_ngcontent-%COMP%]   .search-input[_ngcontent-%COMP%]:focus{border-color:#667eea}.search-container[_ngcontent-%COMP%]   .search-row[_ngcontent-%COMP%]   .search-btn[_ngcontent-%COMP%]{padding:.9rem 1.5rem;background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;border:none;border-radius:.75rem;font-size:1rem;font-weight:600;cursor:pointer;transition:opacity .2s}.search-container[_ngcontent-%COMP%]   .search-row[_ngcontent-%COMP%]   .search-btn[_ngcontent-%COMP%]:hover{opacity:.9}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]{background:linear-gradient(135deg,#667eea,#764ba2);border-radius:1rem;padding:1.75rem;color:#fff;margin-top:.5rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .city-name[_ngcontent-%COMP%]{font-size:2rem;font-weight:700}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .country[_ngcontent-%COMP%]{background:#ffffff40;padding:.25rem .6rem;border-radius:1.25rem;font-size:.85rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .description[_ngcontent-%COMP%]{opacity:.85;margin-top:.25rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .temperature[_ngcontent-%COMP%]{margin:1.25rem 0;display:flex;align-items:baseline;gap:1rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .temperature[_ngcontent-%COMP%]   .temp-main[_ngcontent-%COMP%]{font-size:4rem;font-weight:700}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .temperature[_ngcontent-%COMP%]   .feels-like[_ngcontent-%COMP%]{opacity:.75;font-size:.95rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .weather-details[_ngcontent-%COMP%]{display:flex;gap:1.5rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .weather-details[_ngcontent-%COMP%]   .detail-item[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;background:#ffffff26;padding:.75rem 1.25rem;border-radius:.75rem;gap:.25rem}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .weather-details[_ngcontent-%COMP%]   .detail-item[_ngcontent-%COMP%]   .detail-label[_ngcontent-%COMP%]{font-size:.8rem;opacity:.75}.search-container[_ngcontent-%COMP%]   .weather-card[_ngcontent-%COMP%]   .weather-details[_ngcontent-%COMP%]   .detail-item[_ngcontent-%COMP%]   .detail-value[_ngcontent-%COMP%]{font-size:1.1rem;font-weight:600}.search-container[_ngcontent-%COMP%]   .empty-state[_ngcontent-%COMP%]{text-align:center;color:#9ca3af;padding:2.5rem 1.25rem;font-size:1rem}"]})};var Ll=class e{title="weather-app";currentYear=new Date().getFullYear();static \u0275fac=function(t){return new(t||e)};static \u0275cmp=W({type:e,selectors:[["app-root"]],decls:9,vars:0,consts:[[1,"app-container"],[1,"app-header"],[1,"app-main"]],template:function(t,r){t&1&&(_(0,"div",0)(1,"header",1)(2,"h1"),q(3,"\u{1F324} Weather App"),C(),_(4,"p"),q(5,"Get live weather for any city"),C()(),_(6,"main",2),ae(7,"app-weather-search"),C()(),ae(8,"router-outlet"))},dependencies:[is,Pl],styles:["*[_ngcontent-%COMP%]{box-sizing:border-box;margin:0;padding:0}body[_ngcontent-%COMP%]{font-family:Segoe UI,Arial,sans-serif;background:linear-gradient(135deg,#1e3c72,#2a5298);min-height:100vh}.app-container[_ngcontent-%COMP%]{max-width:800px;margin:auto;padding:2.5rem 1.25rem}.app-header[_ngcontent-%COMP%]{text-align:center;color:#ff9500;margin-bottom:2.5rem}.app-header[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{font-size:2.5rem;font-weight:700;margin-bottom:.5rem}.app-header[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{font-size:1.1rem;opacity:.9}.app-main[_ngcontent-%COMP%]{background:#fff;color:#333;border-radius:1rem;padding:2rem;box-shadow:0 20px 60px #0003}"]})};mp(Ll,s_).catch(e=>console.error(e));
